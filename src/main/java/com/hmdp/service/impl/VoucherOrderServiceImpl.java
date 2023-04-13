package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    SeckillVoucherServiceImpl seckillVoucherService;

    @Resource
    RedisIdWorker redisIdWorker;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Resource
    RedissonClient redissonClient;

    //定义lua脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    //使用静态代码块实现 lua脚本的初始化 这样就只需要加载一次 不用重复加载
    static{
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        // ClassPathResource() resource目录下的文件
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 异步处理线程池
    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    IVoucherOrderService currentProxy;
    //在类初始化之后执行，因为当这个类初始化好了之后，随时都是有可能要执行的
    @PostConstruct
    void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    // --------- BlockingQueue阻塞队列 --------
                    //1.获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
                    //2.创建订单
//                    handlerVoucherOrder(voucherOrder);

                    // --------- redis 消息队列 --------
                    String stream_name = "stream.orders";
                    //1.获取消息队列中的订单信息 xreadgroup group g1 c1  count 1 block 2000 streams s1
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(stream_name, ReadOffset.lastConsumed()) //处理标记的下一条消息
                            );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    //3.解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> map = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);

                    //4.创建订单
                    createVoucherOrder(voucherOrder);

                    //5.应答XACK消息（将处理完的消息移除消息队列）
                    stringRedisTemplate.opsForStream().acknowledge(stream_name, "g1", record.getId());
                } catch (Exception e) {
                    log.error("订单处理异常", e);
                    //异常消息处理（服务器宕机,消息为被处理完标记为当前未处理的位置）
                    handlerPendingList();
                }
            }
        }

        // 处理异常消息
        private void handlerPendingList() {
            while (true) {
                try {
                    String stream_name = "stream.orders";
                    //1.获取消息队列中pendingList中的订单信息 xreadgroup group g1 c1  count 1 block 2000 streams s1
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(stream_name, ReadOffset.from("0"))  //处理标记的当前条消息
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    //3.解析订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> map = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);

                    //4.创建订单
                    createVoucherOrder(voucherOrder);

                    //5.应答XACK消息（将处理完的消息移除消息队列）
                    stringRedisTemplate.opsForStream().acknowledge(stream_name, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending订单异常", e);
                    // 避免频繁处理
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
        }

        //此处出现线程安全问题几乎不可能，订单处理为单线程处理（此处只为严谨）
        private void handlerVoucherOrder(VoucherOrder voucherOrder) {
            //1.获取用户
            Long userId = voucherOrder.getUserId();
            // 2.创建锁对象
            RLock redisLock = redissonClient.getLock("lock:order:" + userId);
            // 3.尝试获取锁
            boolean isLock = redisLock.tryLock();
            // 4.判断是否获得锁成功
            if (!isLock) {
                // 获取锁失败，直接返回失败或者重试
                log.error("不允许重复下单！");
                return;
            }
            try {
                //注意：由于是spring的事务是放在threadLocal中，此时的是多线程，事务会失效
                currentProxy.createVoucherOrder(voucherOrder);
            } finally {
                // 释放锁
                redisLock.unlock();
            }
        }
    }


    // 阻塞队列 队列没消息是阻塞有消息才会执行
    private BlockingQueue<VoucherOrder> orderTasks =new  ArrayBlockingQueue<>(1024 * 1024);

    // redis优化秒杀
    @Override
    public Result secKillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        Long orderId = redisIdWorker.next("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), orderId.toString(), orderId.toString());
        int r = result.intValue();
        //2.判断结果是否为0
        if (r != 0) {
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }
//------------------------ BlockingQueue阻塞队列 ---------------------
        //3.1.创建优惠券订单
//        VoucherOrder voucherOrder = new VoucherOrder();
        //3.2.用户id
//        voucherOrder.setUserId(userId);
        //3.3代金券id
//        voucherOrder.setVoucherId(voucherId);
        //3.3.订单id
//        voucherOrder.setId(orderId);

        //4.将订单(voucherOrder)存入阻塞队列
//        orderTasks.add(voucherOrder);

        //5.获取代理对象(子线程中无法获取到，使用成员变量赋值的方式获取)
//        currentProxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }

    //数据库创建订单
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //5.1.用户id
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            //用户抢购过
            log.error("用户抢购过");
            return;
        }
        //6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();

        if (!success) {
            log.error("库存不足");
            return;
        }
        save(voucherOrder);
    }




//    @Override
//    public Result secKillVoucher(Long voucherId) {
//        //1.获取优惠券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        //2.查看秒杀时间是否开始
//        LocalDateTime beginTime = voucher.getBeginTime();
//        if (beginTime.isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀未开始！");
//        }
//
//        //3.查看秒杀时间是否结束
//        LocalDateTime endTime = voucher.getEndTime();
//        if (endTime.isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已结束！");
//        }
//        //4.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足！");
//        }
//
//        //5.1.用户id
//        Long userId = UserHolder.getUser().getId();
//
//        //在秒杀服务的接口中定义createVoucherOrder方法
//        // 单机服务的处理方式
////        synchronized (userId.toString().intern()) {
////            //获取代理对象（事务） 此处需要在启动类上添加暴露代理对象的注解 否则获取不到
////            IVoucherOrderService currentProxy = (IVoucherOrderService) AopContext.currentProxy();
////            return currentProxy.createVoucherOrder(voucherId);
////        }
//
//
//
//        //分布式的处理方式
//        //创建锁对象  name: 就是锁的对象  此处为同一个用户
//
//        //SimpleRedisLock simpleRedisLock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
//
//        // 使用Redisson处理
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        //获取锁  5s
//        //boolean isLock = simpleRedisLock.tryLock(1200);
//
//        // 等待重试时间 默认不等待  TTL 30 时间单位 秒
//        boolean isLock = lock.tryLock();
//
//        if (!isLock) { //获取锁失败
//            return Result.fail("一个人只允许下一单");
//        }
//        try {
//            //获取代理对象（事务）
//            IVoucherOrderService currentProxy = (IVoucherOrderService) AopContext.currentProxy();
//            return currentProxy.createVoucherOrder(voucherId);
//        } finally {
////            释放锁
//            lock.unlock();
//        }
//
//    }

    /**
     * 一人一单秒杀逻辑
     * 如果在此方法上加锁，那么所有访问该方法的都要加锁，性能会比较差 （锁粒度太大了）
     * 但是此处要解决的是一人一单问题
     * 只需要给不同id的用户访问时加锁
     * @param voucherId
     * @return
     */



//    @Transactional
//    public Result createVoucherOrder(Long voucherId) {
//        //5.1.用户id
//        Long userId = UserHolder.getUser().getId();
//        /**
//         * 事务与锁的问题
//         *
//         * 如果在事务方法中使用了锁，可能会出现锁在事务提交之前被释放的情况。
//         * 这是因为Spring Boot默认使用的是基于代理的事务管理器，而代理对象是基于接口的，它不能截获基于类的方法调用。
//         * 该getResult方法是基于VoucherOrder实现类的，而不是基于VoucherOrder服务接口的（接口中没有这个方法）
//         * 使用事务默认是基于接口创建的代理类，
//         * 所以基于类的事务方法不在代理类中（不基于接口），所以会出现事务失效，
//         * 事务就是AOP，在需要管理的事务方法前后新增处理，而加锁在事务方法内部，当该位置处理完，
//         * 锁就释放了，而后面的AOP方法的提交事务操作就是在，释放锁之后执行的，锁就失效了
//         */
//
//        //此处的锁的对象是，用户id的字符串对象
//        //使用toString 获取的相同id的对象不是同一个对象（toString会重新创建一个对象）
//        //使用intern（）从常量池中获取对象，id值一样获取的就是同一个对象
//        //降低锁粒度
////        synchronized (userId.toString().intern()) {
//            //5.一人一单逻辑
//            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//            if (count > 0) {
//                //用户抢购过
//                return Result.fail("您已经抢过过了！");
//            }
//            //6.扣减库存
//            boolean success = seckillVoucherService.update()
//                    .setSql("stock = stock - 1")
//                    .eq("voucher_id", voucherId).gt("stock", 0)
//                    .update();
//
//            if (!success) {
//                return Result.fail("库存不足！");
//            }
//            //7.创建优惠券订单
//            VoucherOrder voucherOrder = new VoucherOrder();
//            //7.1用户id
//            voucherOrder.setUserId(userId);
//            //7.2代金券id
//            voucherOrder.setVoucherId(voucherId);
//            //7.3订单id
//            long orderId = redisIdWorker.next("order");
//            voucherOrder.setId(orderId);
//
//            save(voucherOrder);
//            //8.返回订单id
//            return Result.ok(orderId);
//        }
//    }
}

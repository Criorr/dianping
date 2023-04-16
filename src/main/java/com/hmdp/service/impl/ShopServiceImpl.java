package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透解决
//        Shop shop = queryWithPassThrough(id);
//      缓存击穿解决（互斥锁）
//        Shop shop = queryWithMutex(id);
        Shop shop = cacheClient.queryWithLogicExpire(
                CACHE_SHOP_KEY, LOCK_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.SECONDS);
        if (shop == null) {
            return Result.fail("未查找到店铺信息！");
        }
        return Result.ok(shop);
    }

    /**
     * 使用逻辑过期解决缓存击穿问题
     * 逻辑时间的长短 要在 数据的实时性 和 命中率中取舍，实时性高则时间要短
     * 使用逻辑过期，存入redis中的热点数据时不会失效的
     * 知识在逻辑上判断是否失效
     * 如果redis未命中直接返回null
     * 命中则判断逻辑时间是否过期，没过期直接返回数据
     * 过期则获取锁，开辟新线程去数据库中查找数据，写入redis中释放锁，本线程则直接返回旧数据
     * 如果为获取锁则直接返回旧数据
     *
     * 对于加锁的部分都要考虑doublecheck，及volition的使用，来大大提升效率
     */
    //创建线程池
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR =
//            Executors.newFixedThreadPool(10);

//    private Shop queryWithLogicExpire(Long id) {
//        String key =  CACHE_SHOP_KEY + id;
//        //1.在redis中查询数据
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2.不存在，返回null
//        if (StrUtil.isBlank(shopJson)) {
//            return null;
//        }
//
//        //3.判断逻辑时间是否过期
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
//        //逻辑过期时间
//        LocalDateTime shopExpireTime = redisData.getExpireTime();
//        //过期时间是否在当前时间之后
//        if (shopExpireTime.isAfter(LocalDateTime.now())) {
//            //3.1没过期
//            return shop;
//        }
//
//        //3.2逻辑时间过期
//        //4.实现缓存重构
//        //4.1.获取互斥锁
//        String lockKey = LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//        //4.2.判断获取锁是否成功
//        if (isLock) {
//            //doubleCheck 双重检查
//            try {
//                shopJson = stringRedisTemplate.opsForValue().get(key);
//                if (!StrUtil.isBlank(shopJson)) {
//                    RedisData newRedisData = JSONUtil.toBean(shopJson, RedisData.class);
//                    LocalDateTime newExpireTime = newRedisData.getExpireTime();
//                    if(newExpireTime.isAfter(LocalDateTime.now())) {
//                        Shop newShop = JSONUtil.toBean((JSONObject) newRedisData.getData(), Shop.class);
//                        return newShop;
//                    }
//                }
//                //4.3.使用线程池，获取线程创建任务
//                CACHE_REBUILD_EXECUTOR.submit(() -> {
//                    try {
//                        //逻辑时间 固定时长 + 随机时长
//                        this.saveShop2Redis(id, 20L + RandomUtil.randomInt(1,5));
//                    } catch (Exception e) {
//                        throw new RuntimeException();
//                    } finally {
//                        unLock(lockKey);
//                    }
//                });
//            } finally {
//                unLock(lockKey);
//            }
//        }
//        //4.4.返回过期的商品信息
//        return shop;
//    }

    //热点数据预热（提前存入redis）
    public void saveShop2Redis(Long id, Long expireSeconds) {
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }



    /**
     * 使用互斥锁解决缓存击穿问题
     * 也就是热点key问题
     * 在一时间某个热点key失效了，这样所有的请求都会访问数据库，造成数据库的崩溃
     * 为防止这一问题，给当redis中的数据不保存在时，
     * |--去数据库查数据，并存入redis这个操作 加锁--|
     * 没拿到互斥锁的会间隔重新获取
     */
//    private Shop queryWithMutex(Long id) {
//        String key =  CACHE_SHOP_KEY + id;
//        //1.在redis中查询数据
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2.存在，返回数据
//        if (StrUtil.isNotBlank(shopJson)) {
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return shop;
//        }
//
//        //不存在有两种情况 null “”
//        if (shopJson != null) {
//            //shopJson为“”
//            return null;
//        }
//
//        //3.实现缓存重构
//        //3.1.获取互斥锁
//        Shop shop = null;
//        try {
//            boolean isLock = tryLock(LOCK_SHOP_KEY + id);
//            //3.2.判断是否获取成功
//            if (!isLock) {
//                //3.3.获取失败
//                Thread.sleep(50);
//                return queryWithMutex(id);
//            }
//            //3.获取成功，根据id去数据库查询
//            shop = getById(id);
//            //4.不存在,返回错误
//            if (shop == null) {
//                //将不存在的key以空值存入redis，并设置TTL
//                stringRedisTemplate.opsForValue()
//                        .set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//                return null;
//            }
//            //5.存在返回数据，并将数存入redis中(将TTL加上随机数，避免缓存雪崩)
//            stringRedisTemplate.opsForValue()
//                    .set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL + RandomUtil.randomInt(1,5), TimeUnit.MINUTES);
//        } catch (InterruptedException e) {
//            throw new RuntimeException();
//        } finally {
//            //6.释放互斥锁
//            unLock(key);
//        }
//        return shop;
//    }

    //获取互斥锁
//    private boolean tryLock(String key) {
//        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
//        //自动拆包会出现空指针问题
//        return BooleanUtil.isTrue(flag);
//    }
//
//    //释放锁
//    private void unLock(String key) {
//        stringRedisTemplate.delete(key);
//    }

    /**
     * 缓存穿透问题
     * 使用缓存空对象（消耗内存，可能会造成缓存短期不一致问题）的方式解决缓存穿透问题
     * 后续可改成 布隆过滤器的方式解决（内存占用少，实现复杂，可能误判）
     * @param id
     * @return
     */
//    private Shop queryWithPassThrough(Long id) {
//        String key =  CACHE_SHOP_KEY + id;
//        //1.在redis中查询数据
//        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        //2.存在，返回数据
//        if (StrUtil.isNotBlank(shopJson)) {
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return shop;
//        }
//
//        //不存在有两种情况 null “”
//        if (shopJson != null) {
//            //shopJson为“”
//            return null;
//        }
//
//        //3.不存在(null)，到数据库中查询数据
//        Shop shop = getById(id);
//        //4.不存在,返回错误
//        if (shop == null) {
//            //将不存在的key以空值存入redis，并设置TTL
//            stringRedisTemplate.opsForValue()
//                    .set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return null;
//        }
//        //5.存在返回数据，并将数存入redis中(将TTL加上随机数，避免缓存雪崩)
//        stringRedisTemplate.opsForValue()
//                .set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL + RandomUtil.randomInt(1,5), TimeUnit.MINUTES);
//        return shop;
//    }


    /**
     * 使用事务，保证操作的完整性
     * @param shop
     * @return
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        //判断shopId是否为空
        Long shopId = shop.getId();
        if (shopId == null) {
            return Result.fail("商铺Id不能为空！");
        }
        //更新数据库
        updateById(shop);
        //删除redis缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + shopId);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1. 判断是否根据坐标查询
        if (x == null && y == null) {
            // 1.1不根据坐标查询
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis 按照距离排序，分页, 结果shopId, distance距离
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance()
                                .limit(end)
                );

        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        // 不判断后面截取会报错 skip个数大于自身长度
        if (from >= list.size()) {
            // 没有下一页了
            return Result.ok(Collections.emptyList());
        }
        // 4.1. 截取from - end 部分
        List<Long> ids = new ArrayList<>();
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        // 根据id获取shop数据
        String idsStr = StrUtil.join(",", ids);
        List<Shop> shops = query()
                .in("id", ids)
                .last("ORDER BY FIELD(id," + idsStr + ")")
                .list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }
}

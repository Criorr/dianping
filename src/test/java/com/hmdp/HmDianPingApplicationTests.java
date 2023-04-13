package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    ShopServiceImpl shopService;

    @Resource
    RedisIdWorker redisIdWorker;


    ExecutorService es = Executors.newFixedThreadPool(500);

    /**
     * 测试全局唯一id生成器
     * 300线程 每个线程生成100个
     * @throws InterruptedException
     */
    @Test
    public void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long order = redisIdWorker.next("order");
                //以十进制形式输出出来的
                System.out.println("id = " + order);
            }
            //将countDownLatch维护的变量-1
            latch.countDown();
        };

        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }

        //只用countDownLatch维护的变量为0时,主线程就不阻塞了
        //应为多线程是异步的，如果不做处理直接记录时间，可能是某个分线程执行完后记录的时间
        //阻塞主线程让分线程先走
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("总用时长：" + (end - begin) + "ms");
    }

    @Test
    public void testShop() {
        shopService.saveShop2Redis(1L, 10L);
    }

}

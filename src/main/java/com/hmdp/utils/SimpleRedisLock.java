package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * SimpleRedisLock  redis实现分布式锁的简单实现
 *
 * 1.获取锁
 *  1.1 通过redis的set nx ex 获取锁并设置TTL时长
 *  1.2 锁的key使用业务（order） + 加锁的对象（此处用户id）
 *  1.3 锁的value使用 当前线程id + UUID  （用来后面的释放锁前的标志判断）
 * 2.释放锁
 *  x 2.1 如果不做释放锁前的判断是不是当前线程的锁（业务阻塞锁超时释放，锁与线程不一致） 导致线程安全问题
 *  x 2.2 如果做了释放锁前的标志判断，可能会出现JVM垃圾回收机制造成阻塞（GC阻塞锁超时释放，锁与线程不一致），
 *  √ 2.3 使用redis的对lua脚本执行的原子性， 将判断 和 释放锁都写入脚本中
 *
 * @author ZhengKai
 * @date 2023/4/9
 */

public class SimpleRedisLock implements ILock{

    public static final String lock_prefix = "lock:";
    // .toString(true) 去掉UUID中的“-”
    public static final String id_prefix = UUID.randomUUID().toString(true) + "-";
    private StringRedisTemplate stringRedisTemplate;
    private String name;
    //定义lua脚本
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    //使用静态代码块实现 lua脚本的初始化 这样就只需要加载一次 不用重复加载
    static{
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        // ClassPathResource() resource目录下的文件
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }



    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    @Override
    public boolean tryLock(long timeoutSec) {
//        获取当前线程id 直接获取的线程id与JVM有关 分布系统中会出现 重复key问题

        String threadId = id_prefix + Thread.currentThread().getId();
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(lock_prefix + name, threadId, timeoutSec, TimeUnit.SECONDS);
//        此处会发生自动拆箱 如果封装类的值为null 拆箱就会空指针
        return BooleanUtil.isTrue(success);
    }


    /**
     * 判断锁标识的时候可能会出现阻塞 , 锁没正常释放， 再执行其他线程是才释放
     * 会出现线程安全问题
     * 是因为判断和删除 两个操作不是一个原子性操作
     * 此处借助Lua脚本 将两个操作执行 借助lua脚本的特性实现原子性操作
     */
    @Override
    public void unlock() {
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(lock_prefix + name),
                id_prefix + Thread.currentThread().getId());
    }



    /**
     * 解决 锁误删问题 （先判断标识 在 进行释放锁）
     *
     * 当线程获取锁后，业务堵塞，导致时间超出锁的TTL时间，锁自动释放了
     * 这时候别的线程获取到锁进行业务执行，当上一个拥塞的线程执行完，就把当前获取锁的线程的锁释放
     * 导致线程安全问题
     *
     * 再删除锁的时候要对锁进行一个判断，如果是别人的锁就什么都不干，只有是自己的锁才删除
     */
//    @Override
//    public void unlock() {
//        // 获取锁标识
//        String id = stringRedisTemplate.opsForValue().get(lock_prefix + name);
//        // 获取当前线程标识
//        String threadId_cur = id_prefix + Thread.currentThread().getId();
//        // 判断标识是否一致
//        if (threadId_cur.equals(id)) {
//            stringRedisTemplate.delete(lock_prefix + name);
//        }
//    }
}

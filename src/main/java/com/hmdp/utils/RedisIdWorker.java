package com.hmdp.utils;

import lombok.val;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;


import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * RedisIdWorker
 * 全局id生成器
 * 1位符号位 + 31位时间戳 + 32为序列号
 * 时间戳31位允许使用69年
 * 序列号允许每秒生成 2^32 个
 * @author ZhengKai
 * @date 2023/3/27
 */

@Component
public class RedisIdWorker {
    StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //初始时间戳
    private final long BEGIN_TIMESTAMP = 1679875200L;
    //序列号位数
    private final int COUNT_BITS = 32;
    public long next(String keyPrefix){
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowTime = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = nowTime - BEGIN_TIMESTAMP;

        //2.生成序列号
        //2.1.获取当天的日期，精确到天(以天来维护比一时间戳的秒来为key生成序列号，更节省了内存，更方便统计)
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        //时间戳左移32位 逻辑或 序列号 完成时间戳和序列号的组合
        return timeStamp << COUNT_BITS | count;
    }

    public static void main(String[] args) {
        LocalDateTime time = LocalDateTime.of(2023, 3, 27, 0, 0);
        long l = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println(l);
    }
}

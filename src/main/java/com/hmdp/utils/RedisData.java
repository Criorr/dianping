package com.hmdp.utils;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 使用逻辑过期方法解决热点key缓存击穿问题
 * 需要在原有的shop类添加一个逻辑过期时间属性
 * 所以创建一个类将shop类聚合进来
 */
@Data
public class RedisData {
    private LocalDateTime expireTime;
    private Object data;
}

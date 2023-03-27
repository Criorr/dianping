package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

/**
 * CacheClient
 *
 * @author ZhengKai
 * @date 2023/3/26
 */
@Component
@Slf4j
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    //创建线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR =
            Executors.newFixedThreadPool(10);

    //将对象转为json，存入redis设置TTL
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    //将对象和逻辑过期时间转为json存入redis
    public void setWithLogicExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //存“”解决缓存穿透解决
    public <R, ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key =  keyPrefix + id;
        //1.在redis中查询数据
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.存在，返回数据
        if (StrUtil.isNotBlank(json)) {
            R r = JSONUtil.toBean(json, type);
            return r;
        }

        //不存在有两种情况 null “”
        if (json != null) {
            //json为“”
            return null;
        }

        //3.到数据库中查询数据
        R r = dbFallback.apply(id);
        //4.不存在,返回错误
        if (r == null) {
            //将不存在的key以空值存入redis，并设置TTL
            this.set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //5.存在返回数据，并将数存入redis中(将TTL加上随机数，避免缓存雪崩)
        this.set(key, r, time + RandomUtil.randomInt(1,5), unit);
        return r;
    }


    public <R, ID> R queryWithLogicExpire(
            String keyPrefix, String lockPrefix,ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key =  keyPrefix + id;
        //1.在redis中查询数据
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.不存在，返回null
        if (StrUtil.isBlank(json)) {
            return null;
        }

        //3.判断逻辑时间是否过期
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        //逻辑过期时间
        LocalDateTime shopExpireTime = redisData.getExpireTime();
        //过期时间是否在当前时间之后
        if (shopExpireTime.isAfter(LocalDateTime.now())) {
            //3.1没过期
            return r;
        }

        //3.2逻辑时间过期
        //4.实现缓存重构
        //4.1.获取互斥锁
        String lockKey = lockPrefix + id;
        boolean isLock = tryLock(lockKey);
        //4.2.判断获取锁是否成功
        if (isLock) {
            //doubleCheck 双重检查
            try {
                json = stringRedisTemplate.opsForValue().get(key);
                if (!StrUtil.isBlank(json)) {
                    RedisData newRedisData = JSONUtil.toBean(json, RedisData.class);
                    LocalDateTime newExpireTime = newRedisData.getExpireTime();
                    if(newExpireTime.isAfter(LocalDateTime.now())) {
                        R r1 = JSONUtil.toBean((JSONObject) newRedisData.getData(), type);
                        return r1;
                    }
                }
                //4.3.使用线程池，获取线程创建任务
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    try {
                        //逻辑时间 固定时长 + 随机时长
                        R newR = dbFallback.apply(id);
                        this.setWithLogicExpire(key, newR, time + RandomUtil.randomInt(1,5), unit);
                    } catch (Exception e) {
                        throw new RuntimeException();
                    } finally {
                        unLock(lockKey);
                    }
                });
            } finally {
                unLock(lockKey);
            }
        }
        //4.4.返回过期的商品信息
        return r;
    }

    private <R, ID> R queryWithMutex(
            String keyPrefix, String lockPrefix ,ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit) {
        String key =  keyPrefix + id;
        //1.在redis中查询数据
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.存在，返回数据
        if (StrUtil.isNotBlank(json)) {
            R r = JSONUtil.toBean(json, type);
            return r;
        }

        //不存在有两种情况 null “”
        if (json != null) {
            //shopJson为“”
            return null;
        }

        //3.实现缓存重构
        //3.1.获取互斥锁
        R r = null;
        try {
            boolean isLock = tryLock(lockPrefix + id);
            //3.2.判断是否获取成功
            if (!isLock) {
                //3.3.获取失败
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, lockPrefix, id, type, dbFallback, time, unit);
            }
            //3.获取成功，根据id去数据库查询
            r = dbFallback.apply(id);
            //4.不存在,返回错误
            if (r == null) {
                //将不存在的key以空值存入redis，并设置TTL
                this.set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //5.存在返回数据，并将数存入redis中(将TTL加上随机数，避免缓存雪崩)
            this.set(key, r, time + RandomUtil.randomInt(1,5), unit);
        } catch (InterruptedException e) {
            throw new RuntimeException();
        } finally {
            //6.释放互斥锁
            unLock(key);
        }
        return r;
    }

    //获取互斥锁
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        //自动拆包会出现空指针问题
        return BooleanUtil.isTrue(flag);
    }

    //释放锁
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }




}

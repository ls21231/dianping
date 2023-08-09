package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
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

import static com.hmdp.utils.RedisConstants.*;

/**
 * @ls
 * @create 2022 -- 11 -- 10
 */
@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 将任意java对象序列化为json并存储在string类型的key中，并且可以设置ttl过期时间
    public void set(String key, Object value, Long expireTime, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),expireTime,unit);
    }

    // 将任意Java对象序列化为json并存储在string类型的key中，并且可以设置逻辑过期时间，用于处理缓存击穿问题
    public void setWithLogicalExpire(String key, Object value, Long expireTime, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(expireTime)));
        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }



    // 根据指定的key查询缓存，并反序列化为指定类型，利用缓存空值的方式解决缓存穿透问题
    public <R,ID> R queryWithPassThrough(
            String prefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long expireTime, TimeUnit unit) {
        // 1. 查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(prefix + id);
        // 2. 判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3. 存在，直接返回
            JSONUtil.toBean(json, type);
            return JSONUtil.toBean(json, type);
        }
        // 判断缓存是否为空值,为了解决缓存穿透,不等于null，就为空值,空值说明数据库里没有该数据，为了解决
        // 缓存穿透问题，将空值返回给用户
        if(json != null) {
            return null;
        }
        // 4. 不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        // 5. 不存在，直接返回
        if (r == null) {
            // 解决缓存穿透问题，将空对象缓存进redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        // 6. 存在，写入redis缓存
        //stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        this.set(prefix,r,expireTime,unit);
        // 7. 返回
        return r;
    }



    // 根据指定的key查询缓存，并反序列化为指定类型，需要利用逻辑过期解决缓存击穿问题
    public <R, ID> R queryWithLogicalExpire(String prefix,ID id,Class<R> type, Function<ID,R> dbFallback,Long time, TimeUnit unit) {
        // 1. 查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(prefix + id);
        // 2. 判断是否存在
        if (StrUtil.isBlank(json)) {
            // 3. 未命中，直接返回
            // 理论上不存在缓存没有命中的情况，热点数据都会提前添加好，该热点数据只在一段时间内进行大量的频繁访问
            // 所以缓存中没有的数据都就直接返回null
            return null;
        }
        // 4. 命中，将json反序列化，判断是否过期
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 4.1 未过期，直接返回
        if(expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }
        // 4.2 过期，需要缓存重建

        // 5 缓存重建
        boolean isLock = tryLock(LOCK_SHOP_KEY + id);
        // 5.1 获取互斥锁，判断是否获取成功
        if(isLock) {
            // TODO 5.2 获取成功，开启独立线程，实现缓存重建
            try {
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    // 重建缓存，开启一个独立线程去完成
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(prefix + id,r1,time,unit);
                });
            } finally {
                // 锁释放
                unLock(LOCK_SHOP_KEY + id);
            }

        }
        // 5.2 获取锁不成功，此时其他的线程拿到了锁，返回过期的店铺信息
        return r;
    }


    // 互斥锁尝试获取锁
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);

    }

    // 释放锁
    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }


}

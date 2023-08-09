package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
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
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
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
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
//        queryWithPassThrough(id);
        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 互斥锁解决缓存击穿问题
        // Shop shop = queryWithMutex(id);
        // 逻辑过期解决缓存击穿问题
        //Shop shop = queryWithLogicalExpire(id);
        if(shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }


    // 使用互斥锁的方式解决缓存击穿问题
    private Shop queryWithMutex(Long id) {
        // 1. 查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2. 判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3. 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断缓存是否为空值,为了解决缓存穿透,不等于null，就为空值
        if(shopJson != null) {
            return null;
        }
        Shop shop = null;
        try {
            // 4. 实现缓存重建
            // 4.1 获取互斥锁
            boolean lock = tryLock(LOCK_SHOP_KEY + id);
            // 4.2 判断是否获取成功
            if(lock != true) {
                // 4.3 失败则休眠重试
                Thread.sleep(50);
                queryWithMutex(id);
            }
            // 4.4 获取成功，查询数据库
            shop = getById(id);
            // 5. 不存在，直接返回
            if (shop == null) {
                // 解决缓存穿透问题，将空对象缓存进redis
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
                return null;
            }
            // 6. 存在，写入redis缓存
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7. 释放锁
            unLock(LOCK_SHOP_KEY + id);
        }
        // 7. 释放锁
        unLock(LOCK_SHOP_KEY + id);
        return shop;
    }

    // 使用逻辑过期的方式解决缓存击穿问题
    public Shop queryWithLogicalExpire(Long id) {
        // 1. 查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2. 判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            // 3. 未命中，直接返回
            // 理论上不存在缓存没有命中的情况，热点数据都会提前添加好，该热点数据只在一段时间内进行大量的频繁访问
            // 所以缓存中没有的数据都就直接返回null
            return null;
        }
        // 4. 命中，将json反序列化，判断是否过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(data, Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 4.1 未过期，直接返回
        if(expireTime.isAfter(LocalDateTime.now())) {
            return shop;
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
                    this.saveShop2Redis(id,30L);
                });
            } finally {
                // 锁释放
                unLock(LOCK_SHOP_KEY + id  );
            }

        }
        // 5.2 获取锁不成功，此时其他的线程拿到了锁，返回过期的店铺信息
        return shop;
    }

    // 解决缓存穿透的代码，查询店铺信息，使用缓存空对象的方式
    public Shop queryWithPassThrough(Long id) {
        // 1. 查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        // 2. 判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3. 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 如果此时缓存中的对应值为 "" 而不是 null，说明这个数据库在也没有，是为了解决缓存穿透问题缓存的一个空对象
        // 判断缓存是否为空值,为了解决缓存穿透,不等于null，就为空值("", 空字符串),空值说明数据库里没有该数据，为了解决
        // 缓存穿透问题，将空值返回给用户
        if(shopJson != null) {
            return null;
        }
        // 4. 不存在，根据id查询数据库
        Shop shop = getById(id);
        // 5. 不存在，直接返回
        if (shop == null) {
            // 解决缓存穿透问题，将空对象缓存进redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,"",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        // 6. 存在，写入redis缓存
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 7. 返回
        return shop;
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

    private void saveShop2Redis(Long id,Long expireSeconds) {
        // 查询店铺数据
        Shop shop = getById(id);
        // 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusMinutes(expireSeconds));
        // 写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1. 先更新数据库
        updateById(shop);
        // 2. 再删除缓存(更新缓存存在较大的线程安全问题)
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}

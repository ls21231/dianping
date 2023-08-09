package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Override
    public Result getList() {
        // 1. 查询缓存里面有没有相应的缓存
        List<String> cacheList = stringRedisTemplate.opsForList().range("cache:list",0,-1);
        // 2. 有缓存，直接返回

        if (!cacheList.isEmpty()) {
            return Result.ok(JSONUtil.toList(cacheList.get(0),ShopType.class));
        }
        // 3. 没有缓存查询数据库
        List<ShopType> typeList = query().orderByAsc("sort").list();
        // 4. 数据库没有，直接返回
        if (typeList == null) {
            return Result.fail("商铺列表不存在");
        }
        // 5. 数据库有，写入redis
        stringRedisTemplate.opsForList().leftPush("cache:list", JSONUtil.toJsonStr(typeList));
        // 6. 返回
        return Result.ok(typeList);
    }



        /*//获取redis中商户
        String shopType = stringRedisTemplate.opsForValue().get("shopType");
        if (StrUtil.isNotBlank(shopType)) {
            //存在，直接返回
            List<ShopType> shopTypes = JSONUtil.toList(shopType, ShopType.class);
            return Result.ok(shopTypes);
        }
        //不存在，从数据库中查询写入redis
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        //不存在，返回错误
        if (shopTypes == null) {
            return Result.fail("分类不存在");
        }
        //将查询到的信息存入redis
        stringRedisTemplate.opsForValue().set("shopType", JSONUtil.toJsonStr(shopTypes));
        //返回
        return Result.ok(shopTypes);
    }*/
}

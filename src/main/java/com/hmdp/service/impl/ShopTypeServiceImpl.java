package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.injector.methods.SelectList;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_LIST_KEY;

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
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        String key = CACHE_SHOP_TYPE_LIST_KEY;
        //1.查询redis
        String shopTypesJson = stringRedisTemplate.opsForValue().get(key);
        //2.存在，返回结果
        if (StrUtil.isNotBlank(shopTypesJson)) {
            List<ShopType> shopTypes = JSONUtil.toList(JSONUtil.parseArray(shopTypesJson), ShopType.class);
            return Result.ok(shopTypes);
        }
        //3.不存在，去数据库查询
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        //4.不存在，返回错误
        if (shopTypes == null || shopTypes.isEmpty()) {
            return Result.fail("未查到店铺类型！");
        }

        //5.存在，存入redis，并返回数据
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shopTypes));
        return Result.ok(shopTypes);

    }
}

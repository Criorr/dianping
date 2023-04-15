package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    UserServiceImpl userService;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result isFollow(Long followUserId) {
        // 1.获取当前用户id
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        // 2.查询当前用户是否关注
        Integer count = query().eq("user_id", userId)
                .eq("follow_user_id", followUserId)
                .count();
        // 3.判断
        return Result.ok(count > 0);
    }

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        // 1.获取当前用户id
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        // 当前用户关注集合的key
        String key = "follows:" + userId;
        // 2.判断是关注还是取关
        if (isFollow) {
            // 3.关注，新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if (isSuccess) {
                // 把关注用户的id存入redis的当前用户的关注集合中 sadd key followUserId
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        } else {
            // 4.取关，删除数据 delete from tb_follow where user_id = ? and follow_user_id = ?
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId)
                    .eq("follow_user_id", followUserId));
            if (isSuccess) {
                // 把关注用户的id从Redis集合中移除
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result followCommons(Long id) {
        // 1. 获取当前用户的id
        Long userId = UserHolder.getUser().getId();

        String key = "follows:" + userId;
        String key2 = "follows:" + id;
        // 2. 获取两个用户共同关注的用户
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if (intersect == null || intersect.isEmpty()) {
            // 没有共同关注
            return Result.ok(Collections.emptyList());
        }
        //解析集合中的id
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());

        //通过id获取用户信息
        List<Object> users = userService.listByIds(ids).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(users);
    }
}

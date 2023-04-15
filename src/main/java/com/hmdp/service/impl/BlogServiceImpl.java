package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {


    @Resource
    private IUserService userService;

    @Resource
    private IFollowService followService;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryBlogById(Long id) {
        //1.查询blog
        Blog blog = getById(id);
        //2.判断blog
        if (blog == null) {
            return Result.fail("笔记不存在！");
        }
        queryBlogUser(blog);
        // 是否点赞
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        //1.获取用户
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        String key = BLOG_LIKED_KEY + id;
        //2.查询redis的set集合中是否存在
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null) {
            // 3.1.未点赞 可以点赞
            // 3.2.更新数据库 like + 1 修改点赞数量
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            // 3.3.将用户id存入redis的set集合
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 4.1.已点赞 取消点赞
            // 4.2.更新数据库 like - 1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            // 4.3.将用户从redis的set集合中删除
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result likesBlogRank(Long id) {
        String key = BLOG_LIKED_KEY + id;
        //1.查询top5的点赞用户
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }
        //2.解析结果中的用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idsStr = StrUtil.join(",", ids);
        //3.根据id查询用户 where id in (1,5) order by field id(id, 5, 1)
        List<UserDTO> userDTOS = userService.query().in("id", ids)
                .last("order by field(id," + idsStr + ")").list().stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //4.返回
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        // 判断是否保存成功
        if (isSuccess) {
            // 成功 获取用户的粉丝列表
            // select * from tb_follow where follow_user_id = ?
            List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
            for (Follow follow : follows) {
                // 粉丝id
                Long userId = follow.getUserId();
                // 被推送笔记的用户标识
                String key = FEED_KEY + userId;
                // 推送 (将笔记id存入要推送用户标识的集合中)
                stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
            }
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogByFollow(Long max, Integer offset) {
        // 1. 获取当前用户信息
        UserDTO user = UserHolder.getUser();
        String key = FEED_KEY + user.getId();
        // 2. 获取用户收件箱数据 ZREVRANGEBYSCORE key  Min Max LIMIT offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 2.1 判空
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 3. 解析数据 blogId、 offset、 minTime
        int os = 1;
        long minTime = 0;
        List<Long> ids = new ArrayList<>(typedTuples.size());
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 获取BlogId
            ids.add(Long.valueOf(typedTuple.getValue()));
            // 获取时间戳
            long time = typedTuple.getScore().longValue();
            if (minTime == time) {
                os++;
            } else {
                minTime = time;
                os = 1;
            }
        }
        // 根据id查询blog
        String idsStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids)
                .last("order by field(id," + idsStr + ")").list();
        for (Blog blog : blogs) {
            // 查询关联用户
            queryBlogUser(blog);
            // 查询是否点赞
            isBlogLiked(blog);
        }

        // 封装并返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(os);
        scrollResult.setMinTime(minTime);
        return Result.ok(scrollResult);
    }

    // 博客关联用户
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    // 是否已点赞
    private void isBlogLiked(Blog blog) {
        String key = BLOG_LIKED_KEY + blog.getId();
        UserDTO user = UserHolder.getUser();
        if (user == null) {
            // 用户未登录，无需查询是否点赞
            return;
        }
        //查询redis的set集合中是否存在
        Double score = stringRedisTemplate.opsForZSet().score(key, user.getId().toString());
        blog.setIsLike(score != null);


    }
}

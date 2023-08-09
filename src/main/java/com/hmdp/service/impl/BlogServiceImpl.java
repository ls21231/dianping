package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
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
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
            queryBolgWithUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        // 1. 查询博客
        Blog blog = getById(id);
        if(blog == null) {
            return Result.fail("博客不存在");
        }
        // 2. 查询blog相关的用户

        queryBolgWithUser(blog);
        // 3. 查询bolg是否被点赞过
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if(user == null) {
            // 用户未登录，不用查询
            return;
        }
        Long userId = user.getId();
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY + blog.getId(), userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        // 1. 获取当前用户
        UserDTO user = UserHolder.getUser();
        // 2. 判断当前用户是否点赞
        Double score = stringRedisTemplate.opsForZSet().score(BLOG_LIKED_KEY + id, user.getId().toString());
        if(score == null) {
            // 3. 如果未点赞
            // 3.1.数据库点赞数 + 1
            boolean success = this.update().setSql("liked = liked + 1").eq("id", id).update();
            // 3.2.将用户保存到set集合
            if(success) {
                stringRedisTemplate.opsForZSet().add(BLOG_LIKED_KEY + id, user.getId().toString(),System.currentTimeMillis());
            }
        } else {
            // 4.如果已点赞
            // 4.1.数据点赞数 - 1
            boolean success = this.update().setSql("liked = liked - 1").eq("id", id).update();
            // 4.2.将用户从set集合中移除
            if(success) {
                stringRedisTemplate.opsForZSet().remove(BLOG_LIKED_KEY + id, user.getId().toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(BLOG_LIKED_KEY + id, 0, 4);
        if(top5 == null || top5.isEmpty()) {
            // 说明没人点赞
            return Result.ok(Collections.emptyList());
        }

        // 解析出top5中的id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());

        String idStr = StrUtil.join(",", ids);
        List<UserDTO> userDTOS = userService
                .query()
                .in("id",ids)
                // 解决点赞顺序不对"ORDER BY FIELD(id," + idStr + ")"
                .last("ORDER BY FIElD(id," + idStr + ")").list()
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBolg(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if(!isSuccess) {
            // 笔记保存失败
            Result.fail("笔记保存失败");
        }
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        for(Follow follow : follows) {
            stringRedisTemplate.opsForZSet().add(FEED_KEY + follow.getUserId(),blog.getId().toString(),System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }


    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询收件箱 ZREVRANGEBYSCORE key Max Min LIMIT offset count
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 3.非空判断
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
        // 4.解析数据：blogId、minTime（时间戳）、offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0; // 2
        int os = 1; // 2
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) { // 5 4 4 2 2
            // 4.1.获取id
            ids.add(Long.valueOf(tuple.getValue()));
            // 4.2.获取分数(时间戳）
            long time = tuple.getScore().longValue();
            if(time == minTime){
                os++;
            }else{
                minTime = time;
                os = 1;
            }
        }

        // 5.根据id查询blog
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        for (Blog blog : blogs) {
            // 5.1.查询blog有关的用户
            queryBolgWithUser(blog);
            // 5.2.查询blog是否被点赞
            isBlogLiked(blog);
        }

        // 6.封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(os);// os是下一次查询的偏移量，封装响应给前端，下次查询的时候携带过来再次查询
        r.setMinTime(minTime);

        return Result.ok(r);
    }

    @Scheduled()
    public void queryBolgWithUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());


    }


}

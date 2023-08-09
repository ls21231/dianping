package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import org.apache.ibatis.mapping.CacheBuilder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

/**
 * @ls
 * @create 2022 -- 11 -- 07
 */
public class RefreshTokenInterceptor implements HandlerInterceptor {

    private StringRedisTemplate redisTemplate;


    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1. 获取session -> 获取请求头中的token
        String token = request.getHeader("authorization");
        if (token==null) {
            return true;
        }
        // 2. 得到user对象 -> 基于token
        Map<Object, Object> userMap = redisTemplate.opsForHash().entries(LOGIN_USER_KEY + token);
        // 3. 判断用户是否存在将user放到 ThreadLocal 中
        if (userMap.isEmpty()) {
            return true;
        }
        // 将查询的Hash数据转为UserDTO
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(),false);
        UserHolder.saveUser(userDTO);
        // 重新设置有效期
        redisTemplate.expire(LOGIN_USER_KEY + token,LOGIN_USER_TTL, TimeUnit.SECONDS);
        // 4. 放行
        return true;

    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 移除用户
        UserHolder.removeUser();
    }

    public RefreshTokenInterceptor(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }
}

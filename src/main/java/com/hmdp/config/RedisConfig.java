package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @ls
 * @create 2022 -- 11 -- 12
 */
@Configuration
public class RedisConfig {

    @Bean
    public RedissonClient redissonClient() {
        // 创建配置类
        Config config = new Config();
        config.useSingleServer().setAddress("redis://iserver:6380");

        // 创建RedissonClient对象
        return Redisson.create(config);
    }
}

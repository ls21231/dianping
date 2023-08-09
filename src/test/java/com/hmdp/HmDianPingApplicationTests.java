package com.hmdp;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Test
    public void test() {

        LocalDateTime now = LocalDateTime.now();
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        // 2.2 自增长
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + "keyPrefix" + ":" + date);
        System.out.println(count);
    }
}

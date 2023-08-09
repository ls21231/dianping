---- 锁的key
--local key = KEY[1]
---- 当前的线程标识
--local threadId = ARGV[1]
-- 获取锁中的线程标识



local id = redis.call('get',KEY[1]);
if(id == ARGV[1]) then
    -- 释放锁 del key
    return redis.call('del',KEY[1])
end
return 0
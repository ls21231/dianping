package com.hmdp.utils;

/**
 * @ls
 * @create 2022 -- 11 -- 12
 */
public interface ILock {
    /**
     * 尝试获取锁
     * @param timeoutSec 所持有的超时时间，过期后自动释放
     * @return true 代表获取锁成功; false代表获取锁失败
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unlock();
}

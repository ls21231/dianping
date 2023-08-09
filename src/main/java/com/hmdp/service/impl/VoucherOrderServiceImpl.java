package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;



    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private IVoucherOrderService proxy;

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherHandler());
    }
    private class VoucherHandler implements Runnable{
        @Override
        public void run() {
            while (true) {
                try {
                    // 获取消息队列中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    // 判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        // 如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 如果获取成功，创建订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> map = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);
                    handlerVoucherOrder(voucherOrder);
                    // ACK确认，从pending-list中将消息移除，防止多次消费
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1",record.getId());
                } catch (Exception e) {
                    // 消息处理一旦出异常，就会加入到pending-list
                    log.error("消息处理异常",e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 获取pending-list中的订单信息
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
                    );
                    // 判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        // 如果获取失败，说明pending-list没有消息，结束循环
                        break;
                    }
                    // 如果获取成功，创建订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> map = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(map, new VoucherOrder(), true);
                    handlerVoucherOrder(voucherOrder);
                    // ACK确认，从pending-list中将消息移除，防止多次消费
                    stringRedisTemplate.opsForStream().acknowledge("stream.orders","g1",record.getId());
                } catch (Exception e) {
                    // 消息处理一旦出异常，就会加入到pending-list
                    log.error("pending-list消息处理异常",e);
                }
            }
        }
    }

    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherHandler implements Runnable{
        @Override
        public void run() {
            while (true) {
                try {
                    // 获取订单中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 创建订单
                    handlerVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }*/



    private void handlerVoucherOrder(VoucherOrder voucherOrder) {
        // 获取用户
        Long userId = voucherOrder.getUserId();
        // 创建由Redisson框架提供的对象
        RLock lock = redissonClient.getLock("order" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        if(!isLock) {
            // 说明获取锁不成功, 此时一般有两种方案，重试或者返回错误信息，当前时秒杀业务，所以返回错误信息
            // return Result.fail("不允许重复下单");
            log.info("几乎不可能出现锁的问题，不上锁也行");
        }
        // 获取代理对象（事务问题）
        try {

            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 不管业务执行成功还是超时，都必须释放锁
            lock.unlock();
        }
    }

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    // 基于redis的stream消息队列
    @Override
    public Result seckillVoucher(Long voucherId) {

        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本,判断用户是否有购买资格，原子性操作
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(), // 集合里面放redis的键
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId));
        // 2.判断返回结果是否为0
        int r = result.intValue();
        if(r != 0) {
            // 2.1.不为0，代表没有购买资格
            return Result.fail(r == 1?"库存不足":"重复下单");
        }
        // 获取代理对象
        this.proxy = (IVoucherOrderService) AopContext.currentProxy();
        return Result.ok(orderId);
    }



    // 基于jvm的阻塞队列
    /*@Override
    public Result seckillVoucher(Long voucherId) {

        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本,判断用户是否有购买资格，原子性操作
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString());
        // 2.判断返回结果是否为0
        int r = result.intValue();
        if(r != 0) {
            // 2.1.不为0，代表没有购买资格
            return Result.fail(r == 1?"库存不足":"重复下单");
        }
        // 2.2.为0， 有购买资格，把下单信息保存到阻塞队列
        long orderId = redisIdWorker.nextId("order");
        //  保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 订单id
        voucherOrder.setId(orderId);
        // 用户id
        voucherOrder.setUserId(userId);
        // voucherId
        voucherOrder.setVoucherId(voucherId);
        orderTasks.add(voucherOrder);
        // 3.返回订单id
        // 获取代理对象
        this.proxy = (IVoucherOrderService) AopContext.currentProxy() ;
        return Result.ok(orderId);
    }*/

    /*public Result seckillVoucher(Long voucherId) {
        // 1. 查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2. 判断秒杀是否开始
        if(voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }
        // 3. 判断秒杀是否结束
        if(voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经开始");
        }
        // 4. 判断库存是否足够
        if(voucher.getStock() < 1) {
            return Result.fail("库存不足");
        }

        Long userId = UserHolder.getUser().getId();
        
        // 这样只能解决单体项目的一人一单问题，当项目部署为分布式项目时，不同的机器上维护者不同的锁监视器
        // 使得在不同的机器上还是可以出现一人下了多次但的情况，所以要使用分布式锁，有多种方案，这里选择redis做分布式锁
        //synchronized (userId.toString().intern()) 
        
        
        // 创建锁对象,这是自己实现的锁对象
        // SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order" + userId);
        // 创建由Redisson框架提供的对象
        RLock lock = redissonClient.getLock("order" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        if(!isLock) {
            // 说明获取锁不成功, 此时一般有两种方案，重试或者返回错误信息，当前时秒杀业务，所以返回错误信息
            return Result.fail("不允许重复下单");
        }
        // 获取代理对象（事务问题） 因为是对createVoucherOrder方法做的事务，必须由它的代理对象调用才有事务功能
        try {
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 不管业务执行成功还是超时，都必须释放锁
            lock.unlock();
        }
    }*/

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 一人一单,根据userId和voucherId查询用户是否下过单 

        // 执行当前业务的是子线程，无法得到
        Long userId = voucherOrder.getUserId();

        // synchronized (userId.toString().intern())  不能在这个地方上锁，范围太小了
        // 在事务为提交之前，在数据未写入到数据库之前，再有线程进行查询，仍可造成并发问题,
        // 所以，在方法外部进行上锁，在事务提交之后再释放锁
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {

           log.info("用户已经购买过一次了");
        }
        // 5. 扣减库存,使用乐观锁解决超卖问题
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        // 6. 创建订单
        if (success) {

            save(voucherOrder);
        }
    }
}

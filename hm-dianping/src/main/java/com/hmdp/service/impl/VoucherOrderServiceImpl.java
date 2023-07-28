package com.hmdp.service.impl;


import com.hmdp.config.RedissonConfig;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Autowired
    private SeckillVoucherServiceImpl seckillVoucherService;

    @Autowired
    private RedisIdWorker redisIdWorker;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedissonClient redissonClient;
    @Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠券信息
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        LocalDateTime beginTime = seckillVoucher.getBeginTime();
        LocalDateTime endTime = seckillVoucher.getEndTime();
        //2.判断秒杀是否开始
        if(LocalDateTime.now().isBefore(beginTime)){
            return Result.fail("活动还未开始");
        }
        //3.判断秒杀是否结束
        if(LocalDateTime.now().isAfter(endTime)){
            return Result.fail("活动已经结束");
        }
        //4.判断库存是否充足
        Integer stock = seckillVoucher.getStock();
        if(stock <=0){
            return Result.fail("秒杀优惠券库存不足");
        }
        //用户id
        UserDTO userDTO = UserHolder.getUser();
        Long userid = userDTO.getId();

        //创建锁对象
        //SimpleRedisLock redisLock =new SimpleRedisLock(stringRedisTemplate,"order");
        //一人一单，所以是对userid上锁
        RLock redisLock = redissonClient.getLock("order" + userid);

        boolean flag = redisLock.tryLock();

        if(!flag){
            return Result.fail("不能重复购买");
        }
        //这样做是先提交事务再释放锁，不然如果先释放锁再提交事务的话，事务还未提交，数据还未写入数据库，锁就释放了，
        // 其他线程查询到依然为0，又出现了线程安全问题
        //synchronized(userid.toString().intern())
        //{

        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }
        finally {
            redisLock.unlock();
        }
        //}
    }

    @Transactional //事务
    public Result createVoucherOrder(Long voucherId) {
        //用户id
        UserDTO userDTO = UserHolder.getUser();
        Long userid = userDTO.getId();
        //5.一人一单
        //查询优惠券订单
            int count = query().eq("voucher_id", voucherId).eq("user_id", userid).count();
            //判断数量
            if (count > 0) {
                return Result.fail("您已重复购买！");
            }
            //6.扣减库存
            boolean flag = seckillVoucherService.update()  //mybatis-plus
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0)
                    .update();

            if (!flag) {
                return Result.fail("秒杀优惠券库存不足");
            }

            //7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            //订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);

            voucherOrder.setUserId(userid);
            //代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);
            //8.返回订单id
            return Result.ok(orderId);
    }
}

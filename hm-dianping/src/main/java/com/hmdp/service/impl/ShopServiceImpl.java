package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private CacheClient client;
    @Override
    public Result queryById(Long id) {
        //解决缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //Shop shop = client.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //利用互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        //利用逻辑过期解决缓存击穿
        Shop shop = client.queryWithLogicExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 10L, TimeUnit.SECONDS);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        //返回
        return Result.ok(shop);
    }
    //mutex  互斥锁
    public Shop queryWithMutex(Long id){
        String key=CACHE_SHOP_KEY+id;
        //从redis中查询店铺缓存
        String shopCache = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isNotBlank(shopCache)){
            //如果存在，直接返回
            Shop shop = JSONUtil.toBean(shopCache, Shop.class);
            return shop;
        }
        if(shopCache !=null){
            //返回错误信息
            return null;
        }
        //店铺锁
        String lockKey = "lock:shop:"+id;
        //4.实现缓存重建
        Shop shop = null;
        try {
            //4.1获取互斥锁
            boolean tryLockBoolean = tryLock(lockKey);
            //4.2 判断是否获取锁成功
            if(!tryLockBoolean){
                //4.3失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            //4.4成功索取锁，则查询数据库
            shop = getById(id);
            //模拟查询复杂数据库操作，加时间
            Thread.sleep(200);
            //5. 如果数据库也不存在，设置店铺为""解决缓存穿透
            if(shop == null){
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6 存在则存入redis
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unlock(lockKey);
        }
        //返回
        return shop;
    }

    //设置锁
    private boolean tryLock(String key){
        Boolean aBoolean = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(aBoolean);
    }
    //释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    //定义一个线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    //利用逻辑过期解决缓存击穿
    public Shop queryWithLogicExpire(Long id){
        String key=CACHE_SHOP_KEY+id;
        //1.从redis中查询店铺缓存
        String shopCache = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isBlank(shopCache)){
            //3.如果不存在，直接返回
            return null;
        }
        //4.存在，先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopCache, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断店铺缓存是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1.未过期，直接返回店铺信息
            return shop;
        }
        //5.2过期，进行缓存重建.
        //6.缓存重建
        //6.1获取互斥锁
        String lockKey = "lock:shop:"+id;

        boolean isLock = tryLock(lockKey);
        //6.2判断是否获取锁成功
        if (isLock){
            //6.3获取锁成功 ，开启独立线程进行缓存重建
            CACHE_REBUILD_EXECUTOR.submit(()->{
                try {
                    saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放🔒
                    unlock(lockKey);
                }
            });
        }
        //6.4 不成功，返回过期店铺信息
        return shop;
    }

    //逻辑过期：将热点店铺存入redis，带上逻辑过期时间
    public void saveShop2Redis(Long id , Long expireTime) throws InterruptedException {
        //查询店铺数据
        Shop shop = getById(id);
        Thread.sleep(200);
        //封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireTime));
        //存入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(redisData));
    }

    //解决缓存穿透，减缓缓存穿透，对mysql数据库和redis都查不到的设置为“”（空字符串）
    public Shop queryWithPassThrough(Long id){
        String key=CACHE_SHOP_KEY+id;
        //从redis中查询店铺缓存
        String shopCache = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isNotBlank(shopCache)){
            //如果存在，直接返回
            Shop shop = JSONUtil.toBean(shopCache, Shop.class);
            return shop;
        }
        if(shopCache !=null){
            //返回错误信息
            return null;
        }
        //如果不存在，从数据库从查询
        Shop shop = getById(id);
        //如果数据库中不存在，返回错误
        if(shop == null){
            //减缓缓存穿透，对数据库查不到的设置为“”（空字符串）
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,"",2, TimeUnit.MINUTES);
            return null;
        }
        //如果数据库中存在，写入redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),30, TimeUnit.MINUTES);
        //返回
        return shop;
    }



    @Override
    @Transactional
    public Result update(Shop shop) {

        Long id = shop.getId();
        if(id == null){
            System.out.println("店铺id不能为空");
        }
        //先操作数据库
        updateById(shop);
        //再删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY+id);
        return Result.ok();
    }
}

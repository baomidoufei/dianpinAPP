package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    //为缓存设置过期时间
    public void set(String key , Object value , Long time , TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    //设置逻辑过期时间
    public void setWithLogicExpire(String key , Object value , Long time , TimeUnit unit){
        RedisData redisData =new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(redisData));
    }


    //解决缓存穿透，减缓缓存穿透，对mysql数据库和redis都查不到的设置为“”（空字符串）
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,
                                         Long time , TimeUnit unit){
        String key=keyPrefix+id;
        //从redis中查询店铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if(StrUtil.isNotBlank(json)){
            //如果存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        if(json !=null){
            //返回错误信息
            return null;
        }
        //如果不存在，从数据库从查询
        R r = dbFallback.apply(id);
        //如果数据库中不存在，返回错误
        if(r == null){
            //减缓缓存穿透，对数据库查不到的设置为“”（空字符串）
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY+id,"",2, TimeUnit.MINUTES);
            return null;
        }
        //如果数据库中存在，写入redis
        this.set(key,r,time,unit);
        //返回
        return r;
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
    public <R,ID> R queryWithLogicExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,
                                     Long time , TimeUnit unit){
        String key=keyPrefix+id;
        //1.从redis中查询店铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2.判断是否存在
        if(StrUtil.isBlank(json)){
            //3.如果不存在，直接返回
            return null;
        }
        //4.存在，先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        //5.判断店铺缓存是否过期
        if(expireTime.isAfter(LocalDateTime.now())){
            //5.1.未过期，直接返回店铺信息
            return r;
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
                    //查询数据库
                    R r1 = dbFallback.apply(id);
                    //写入redis
                    this.setWithLogicExpire(key,r1,time,unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    //释放🔒
                    unlock(lockKey);
                }
            });
        }
        //6.4 不成功，返回过期店铺信息
        return r;
    }

}

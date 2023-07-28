package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private StringRedisTemplate redisTemplate;

    private String name;
    private static final String Key_Prefix = "lock:";
    private static final String ID_Prefix = UUID.randomUUID(true).toString()+"-";
    //定义lua脚本
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPTS;
    static {
        UNLOCK_SCRIPTS =new DefaultRedisScript<>();
        UNLOCK_SCRIPTS.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPTS.setResultType(Long.class);
    }

    public SimpleRedisLock(StringRedisTemplate redisTemplate, String name) {
        this.redisTemplate = redisTemplate;
        this.name = name;
    }

    @Override
    public boolean tryLock(long time) {
        //线程标识
        String threadId = ID_Prefix+Thread.currentThread().getId();
        Boolean aBoolean = redisTemplate.opsForValue()
                .setIfAbsent(Key_Prefix + name, threadId.toString(), time, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(aBoolean);
    }

    @Override
    public void unlock() {
        redisTemplate.execute(UNLOCK_SCRIPTS,
                Collections.singletonList(Key_Prefix + name),
                ID_Prefix+Thread.currentThread().getId());



        /*String threadId = ID_Prefix+Thread.currentThread().getId();
        String id= redisTemplate.opsForValue().get(Key_Prefix + name);
        if(threadId.equals(id)){
            redisTemplate.delete(Key_Prefix + name);*/
    }
}

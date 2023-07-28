package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result likedBlog(Long id) {
        //获取当前用户id
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        //判断用户是否在set集合里面
        String key ="blog:liked"+id;
        Boolean ismember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        //如果不存在，则可以点赞
        if(BooleanUtil.isFalse(ismember)){
            boolean isSuccess = update().setSql("liked =liked+1").eq("id", id).update();
            //并且把用户添加到set集合里面
            if(BooleanUtil.isTrue(isSuccess)) {
                stringRedisTemplate.opsForSet().add(key, userId.toString());
            }
        }else{
            boolean isSuccess = update().setSql("liked =liked-1").eq("id", id).update();
            if(BooleanUtil.isTrue(isSuccess)) {
                stringRedisTemplate.opsForSet().remove(key, userId.toString());
            }
        }


        //如果存在，则取消点赞
        //并且从set集合里面移除
        return Result.ok();
    }
}

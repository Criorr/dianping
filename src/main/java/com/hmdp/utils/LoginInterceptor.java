package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

/**
 * LoginInterceptor
 *
 * @author ZhengKai
 * @date 2023/3/19
 */
public class LoginInterceptor implements HandlerInterceptor {
    /**
     *  此处的拦截器不属于springboot管理，所以此处使用@Resource不能注入bean,
     *  但是管理该拦截器类的WebMvcConfig类由springboot管理
     *  在注册拦截器的时候，将需要注入的bean放入，以构造器的方式赋值
     */
    private StringRedisTemplate stringRedisTemplate;

    public LoginInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1.获取session
//        HttpSession session = request.getSession();

        //2.从session中获取用户信息
//        Object user = session.getAttribute("user");


        //1.获取请求头中的token(前端放入的)
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            return false;
        }

        //2.从redis中通过token获取用户信息
        String tokenKey = LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash()
                .entries(tokenKey);


        //3.判断用户是否存在
        if (userMap.isEmpty()) {
            //4.不存在拦截请求，返回401状态码(用户未登录)
            response.setStatus(401);
            return false;
        }
        //4 将map转为bean
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);

        //5.存在，将用户放入ThreadLocal中
        UserHolder.saveUser(userDTO);

        //6.刷新token有效时长
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);

        //7.放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        //移除用户
        UserHolder.removeUser();
    }
}

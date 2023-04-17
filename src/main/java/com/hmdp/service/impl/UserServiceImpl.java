package com.hmdp.service.impl;

import cn.hutool.Hutool;
import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import com.hmdp.utils.SystemConstants;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    /**
     * 使用redis 代替 session 解决 多台tomcat的共享session问题
     * @param phone
     * @param session
     * @return
     */

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.验证手机号时候有效
        if (!RegexUtils.isCodeInvalid(phone)) {
            //无效手机号
            return Result.fail("手机号无效！");
        }
        //2.生成验证码
        String code = RandomUtil.randomNumbers(6);

        //3.保存验证码到session中(将手机号和验证码以k/v的形式存储进session)
//        session.setAttribute(phone, code);

        //3.保存验证码到redis中(key:  login:code:phone)
        stringRedisTemplate.opsForValue()
                .set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);

        //4.发送验证码 (此处模拟调用了第三方api)
        log.debug("验证码发送成功，验证码：{}", code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        //1.再次验证手机号
        if (!RegexUtils.isCodeInvalid(phone)) {
            //无效手机号
            return Result.fail("手机号无效！");
        }

        //2.验证验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !code.equals(cacheCode)) {
            //不一致，报错
            return Result.fail("验证码错误！");
        }

        //3.一致，通过手机号查询用户
        User user = query().eq("phone", phone).one();

        //4.判断用户是否存在
        if (user == null) {
            //5.用户不存在，创建用户
            user = createUserWithPhone(phone);
        }

        //5.将用户保存到session中
//        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));

        //5. 随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString();

        //6.将用户保存到redis中,使用hash数据结构
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //6.1 将userDTO转成Map对象，其中（Long）id 以string方式存储，因为stringRedisTemplate对象只能存string
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));

        //6.2 存储user到redis中
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);

        //6.3 设置token的有效时长（session默认有效时长30min）
        //此处是无操作30min失效，所以每次活动都需要刷新TTL，在拦截器中处理
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);

        //7. 返回token,交给前端，存入浏览器的cookie中
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 1. 获取当前用户id
        Long userId = UserHolder.getUser().getId();
        // 2. 获取日期 yyyyMM
        LocalDateTime now = LocalDateTime.now();
        // 3. 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId.toString() + keySuffix;
        // 4. 获取今天是当月的第几天
        int day = now.getDayOfMonth();
        // 5. 写入redis中 SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key, day - 1, true);
        return Result.ok();
    }

    /**
     *
     * @return 本月连续签到天数
     */
    @Override
    public Result signCount() {
        // 1. 获取当前用户id
        Long userId = UserHolder.getUser().getId();
        // 2. 获取日期 yyyyMM
        LocalDateTime now = LocalDateTime.now();
        // 3. 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId.toString() + keySuffix;
        // 4. 获取今天是当月的第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5. 获取本月第一天到今天的签到记录 返回的是一个十进制的数字 BITFIELD sign:5:202203 GET u14 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        // 判空
        if (result == null || result.isEmpty()) {
            // 没有任何签到结果
            return Result.ok(0);
        }

        // 结果为List因为bitfield 可以同时多个操作（增删改）
        Long num = result.get(0);
        if (num == null || num == 0) {
            // 没有签到
            return Result.ok(0);
        }
        int count = 0;
        // 6.循环遍历
        while (true) {
            // 6.1 让这个数与1 做与运算，得到最后一位数  判断这个数是否为0
            if ((num & 1) == 0) {
                // 如果为0 代表签到中断 退出循环
                break;
            } else {
                // 如果为1 代表签到 计数器+1
                count++;
            }
            // 将数字右移一位，抛弃最后一个bit位，继续下一个bit位
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        //1.创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        //2.保存到数据库
        save(user);
        return user;

    }

}

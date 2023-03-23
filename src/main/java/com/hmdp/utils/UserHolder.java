package com.hmdp.utils;

import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;


/**
 * 使用UserDTO(id，别名，头像)是为了隐藏用户敏感信息，
 * 使用User会将用户所有信息传给前端。
 */
public class UserHolder {
    private static final ThreadLocal<UserDTO> tl = new ThreadLocal<>();

    public static void saveUser(UserDTO user){
        tl.set(user);
    }

    public static UserDTO getUser(){
        return tl.get();
    }

    public static void removeUser(){
        tl.remove();
    }
}

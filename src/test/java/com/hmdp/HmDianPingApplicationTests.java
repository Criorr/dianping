package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;




@SpringBootTest
class HmDianPingApplicationTests {
    @Resource
    ShopServiceImpl shopService;

    @Test
    public void testShop() {
        shopService.saveShop2Redis(1L, 10L);
    }

}

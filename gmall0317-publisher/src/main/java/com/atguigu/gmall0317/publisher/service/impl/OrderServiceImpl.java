package com.atguigu.gmall0317.publisher.service.impl;

import com.atguigu.gmall0317.publisher.dao.OrderWideMapper;
import com.atguigu.gmall0317.publisher.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class OrderServiceImpl  implements OrderService {

   @Autowired
   OrderWideMapper orderWideMapper;

    @Override
    public BigDecimal getOrderAmountTotal(String date) {
        return orderWideMapper.selectOrderWideTotal(date);
    }

    @Override
    public Map<String, BigDecimal> getOrderAmountHour(String date) {
     // 期望结构： {"12":102.00,"13":1231.00....}

        // 目前结构：[{"hr":12,"orderAmount":102.00},{"hr":13,"orderAmount":1231.00},....]
        List<Map> mapList = orderWideMapper.selectOrderAmountHour(date);
        Map<String,BigDecimal> orderMap=new HashMap<>();
        for (Map hourMap : mapList) {
            orderMap.put( String.valueOf( hourMap.get("hr") ), (BigDecimal) hourMap.get("order_amount") );
        }

        return orderMap;
    }
}

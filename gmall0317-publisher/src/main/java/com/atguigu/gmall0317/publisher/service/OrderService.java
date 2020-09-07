package com.atguigu.gmall0317.publisher.service;

import java.math.BigDecimal;
import java.util.Map;

public interface OrderService {

    public BigDecimal getOrderAmountTotal(String date);

    // 查询交易额分时数据
    public Map<String,BigDecimal> getOrderAmountHour(String date);

}

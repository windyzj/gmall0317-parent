package com.atguigu.gmall0317.publisher.dao;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderWideMapper {

    public BigDecimal  selectOrderWideTotal(String date);

    public List<Map> selectOrderAmountHour(String date);

}

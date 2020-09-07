package com.atguigu.gmall0317.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0317.publisher.service.DauService;
import com.atguigu.gmall0317.publisher.service.OrderService;
import com.atguigu.gmall0317.publisher.service.impl.DauServiceImpl;
import io.searchbox.client.config.exception.CouldNotConnectException;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class ChartController {

    @Autowired
    DauService dauService ;

    @Autowired
    OrderService orderService;

    @RequestMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String dt){
            //查询数据库 得到 结果
        List<Map<String,Object>>  rsList=new ArrayList<>();
        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        Long dauTotal=0L;
        try{
          dauTotal = dauService.getDauTotal(dt);
        }catch (Exception e){
            e.printStackTrace();
        }
        dauMap.put("value", dauTotal);
        rsList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value", 233);
        rsList.add(newMidMap);

        Map orderAmountMap=new HashMap();
        BigDecimal orderAmount= orderService.getOrderAmountTotal(dt);

        orderAmountMap.put("id","order_amount");
        orderAmountMap.put("name","新增交易额");
        orderAmountMap.put("value", orderAmount);
        rsList.add(orderAmountMap);

        return JSON.toJSONString(rsList);
    }

    @RequestMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date ){
        String rsJson=null;

        if("dau".equals(id)){
            Map<String,Map<String,Long>> rsMap=new HashMap<>();
            Map<String,Long> tdMap=  dauService.getDauHour(date);
            rsMap.put("today",tdMap);

            Map<String,Long> ydMap=  dauService.getDauHour(getYd(date));
            rsMap.put("yesterday",ydMap);
            rsJson=JSON.toJSONString(rsMap) ;
        }else if("order_amount".equals(id)){
            Map<String,Map<String,BigDecimal>> rsMap=new HashMap<>();
            Map<String,BigDecimal> tdMap=  orderService.getOrderAmountHour(date);
            rsMap.put("today",tdMap);

            Map<String,BigDecimal> ydMap=  orderService.getOrderAmountHour(getYd(date));
            rsMap.put("yesterday",ydMap);
            rsJson=JSON.toJSONString(rsMap) ;
        }

        return rsJson;
    }

    private String getYd(String td){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date date = dateFormat.parse(td);
            Date yDate = DateUtils.addDays(date, -1);
            return  dateFormat.format(yDate);

        } catch (ParseException e) {
            e.printStackTrace();
            throw  new RuntimeException("格式转换异常");
        }


    }


}

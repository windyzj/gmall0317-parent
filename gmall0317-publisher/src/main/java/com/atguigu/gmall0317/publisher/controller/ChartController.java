package com.atguigu.gmall0317.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0317.publisher.service.DauService;
import com.atguigu.gmall0317.publisher.service.impl.DauServiceImpl;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class ChartController {

    @Autowired
    DauService dauService ;

    @RequestMapping("realtime-total")
    public String getRealtimeTotal(@RequestParam("date") String dt){
            //查询数据库 得到 结果
        List<Map<String,Object>>  rsList=new ArrayList<>();
        Map dauMap=new HashMap();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");

        Long dauTotal = dauService.getDauTotal(dt);
        dauMap.put("value", dauTotal);
        rsList.add(dauMap);

        Map newMidMap=new HashMap();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value", 233);
        rsList.add(newMidMap);

        return JSON.toJSONString(rsList);
    }

    @RequestMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date ){
        Map<String,Map<String,Long>> rsMap=new HashMap<>();
        Map<String,Long> tdMap=  dauService.getDauHour(date);
        rsMap.put("today",tdMap);

        Map<String,Long> ydMap=  dauService.getDauHour(getYd(date));
        rsMap.put("yesterday",ydMap);

        return  JSON.toJSONString(rsMap) ;
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

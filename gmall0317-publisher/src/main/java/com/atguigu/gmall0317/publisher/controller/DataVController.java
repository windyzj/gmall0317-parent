package com.atguigu.gmall0317.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0317.publisher.service.TrademarkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class DataVController {

    @Autowired
    TrademarkService trademarkService;

  //  /trademarkstat?dt=xxx
    @RequestMapping("trademarkstat")
    public String trademarkstat(@RequestParam("dt") String date){
        List<Map> trademarkStat = trademarkService.getTrademarkStat(date);
        List<Map> datavList=new ArrayList<>();

        //根据datav所需结构进行调整
        for (Map trademarkMap : trademarkStat) {
             Map<String, Object> datavMap = new HashMap<>();
            datavMap.put("x",trademarkMap.get("trademark_name"));
            datavMap.put("y",trademarkMap.get("amount"));
            datavMap.put("s","1");
            datavList.add(datavMap);
        }

        return JSON.toJSONString(datavList);

    }


}

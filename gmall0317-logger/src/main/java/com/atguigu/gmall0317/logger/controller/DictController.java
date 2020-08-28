package com.atguigu.gmall0317.logger.controller;

import com.sun.deploy.net.HttpResponse;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.util.Date;

@RestController
public class DictController {


    @RequestMapping("/dict")
    public String getDict(HttpServletResponse  httpResponse){
        httpResponse.addHeader("Last-Modified",new Date().toString());
        return "蓝瘦香菇";
    }

}

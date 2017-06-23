package cn.spark.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Created by Administrator on 2017/06/23.
 */
@Controller
public class IndexController {
//    @Autowired
//    private KafkaTemplate kafkaTemplate;
    @RequestMapping
    @ResponseBody
    public String index(){
//        kafkaTemplate.send("test","hello,world");
        return  "hello,world";
    }
}

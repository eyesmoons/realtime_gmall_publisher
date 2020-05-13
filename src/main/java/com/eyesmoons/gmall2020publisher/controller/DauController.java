package com.eyesmoons.gmall2020publisher.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.eyesmoons.gmall2020publisher.service.DauService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class DauController {
    @Autowired
    private DauService dauService;

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam("date") String date){
        Long dauTotal = dauService.getDauTotal(date);
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        Map<String,Object> map = new HashMap<>();

        map.put("id","dau");
        map.put("name","新增日活");
        map.put("value",dauTotal);
        list.add(map);

        Map<String,Object> map2 = new HashMap<>();
        map2.put("id","dau");
        map2.put("name","新增设备");
        map2.put("value",200);
        list.add(map2);
        return JSON.toJSONString(list);
    }

    @RequestMapping("realtime-hours")
    public String realtimeHourDate(@RequestParam("id") String id,@RequestParam("date") String date){
        JSONObject json = new JSONObject();
        if ("dau".equals(id)) {

            Map<String, Long> todayMap = dauService.getDauHour(date);
            json.put("today",todayMap);

            Date dateToday = null;
            try {
                dateToday = new SimpleDateFormat("yyyy-MM-dd").parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            Date dateYesterday = DateUtils.addDays(dateToday, -1);
            String yesterdayDateString=new SimpleDateFormat("yyyy-MM-dd").format(dateYesterday);

            Map<String, Long> yesterdayMap = dauService.getDauHour(yesterdayDateString);
            json.put("yesterday",yesterdayMap);
        }
        return json.toJSONString();
    }
}

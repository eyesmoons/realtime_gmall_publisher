package com.eyesmoons.gmall2020publisher.service;

import java.util.Map;

public interface DauService {
    /**
     * 获取日活总数
     * @param date
     * @return
     */
    public Long getDauTotal(String date);

    /**
     * 分时统计
     * @param date
     * @return
     */
    public Map<String,Long> getDauHour(String date);
}

package com.spring.es.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.spring.es.model.entity.TrafficInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 类描述：
 */
public class CommonUtils {

    public static List<TrafficInfo> parsingJsonData(String jsonData) {
        return parsingJsonData(JSON.parseObject(jsonData));
    }

    /**
     * 解析 JSON
     *
     * @param jsonObject
     * @return
     */
    @SuppressWarnings(value = "all")
    public static List<TrafficInfo> parsingJsonData(JSONObject jsonObject) {
        List<TrafficInfo> trafficInfos = new ArrayList<>();
        for (Map.Entry<String, Object> stringMapEntry : jsonObject.entrySet()) {

            // 出发站点
            String siteStartAndEnd = stringMapEntry.getKey();
            String trafficInfoStr = siteStartAndEnd.trim();

            Map<String, Object> stateMap = (Map<String, Object>) stringMapEntry.getValue();
            // 日期
            for (Map.Entry<String, Object> mapEntry : stateMap.entrySet()) {
                String departureDate = mapEntry.getKey();
                // 航班信息列表
                trafficInfoStr = trafficInfoStr.concat(mapEntry.getKey().trim());
                Map<String, Object> dateMap = (Map<String, Object>) mapEntry.getValue();

                for (Map.Entry<String, Object> stringListEntry : dateMap.entrySet()) {
                    String flightNumber = stringListEntry.getKey();
                    trafficInfoStr = trafficInfoStr.concat(stringListEntry.getKey());
                    JSONArray jsonArray = JSONArray.parseArray(JSON.toJSONString(stringListEntry.getValue()));
                    for (Object map : jsonArray) {
                        Map<String, String> value = (Map<String, String>) map;
                        TrafficInfo trafficInfo = new TrafficInfo();
                        trafficInfo.setSiteStartAndEnd(siteStartAndEnd);
                        trafficInfo.setDepartureDate(departureDate);
                        trafficInfo.setFlightNumber(flightNumber);
                        trafficInfo.setTrafficInfo(trafficInfoStr);
                        trafficInfo.setDataTime(Long.parseLong(value.get("s")));
                        trafficInfo.setPrice(value.get("p"));
                        trafficInfo.setClassCode(value.get("c"));
                        trafficInfo.setNumberOfCabins(Objects.nonNull(value.get("t")) ? Integer.parseInt(value.get("t")) : null);
                        trafficInfo.setCreateTime(System.currentTimeMillis());
                        trafficInfos.add(trafficInfo);
                    }
                }
            }
        }

        return trafficInfos;

    }
}

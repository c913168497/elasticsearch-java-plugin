package com.spring.es.model.entity;

import com.spring.es.constants.IndexConstants;
import com.spring.es.plugin.annotations.Document;
import com.spring.es.plugin.annotations.FieldType;
import com.spring.es.plugin.annotations.Property;
import lombok.Getter;
import lombok.Setter;

/**
 * 类描述：
 */

@Getter
@Setter
@Document(indexName = IndexConstants.TRAFFIC_INDEX_NAME, type = IndexConstants.TRAFFIC_TYPE_NAME, shards = 1, replicas = 0, refreshInterval = "2s")
public class TrafficInfo {

    /**
     * 站台起点终点 （携带分词功能）
     */
    @Property(type = FieldType.Text, analyzer = "charSplit")
    private String siteStartAndEnd;

    /**
     * 出发日期
     */
    @Property(type = FieldType.Keyword)
    private String departureDate;


    /**
     * 航班号
     */
    @Property(type = FieldType.Keyword)
    private String flightNumber;


    /**
     * 时间戳
     */
    @Property(type = FieldType.Long)
    private Long dataTime;

    /**
     * 价格
     */
    @Property(type = FieldType.Keyword)
    private String price;

    /**
     * 舱位数
     */
    @Property(type = FieldType.Integer)
    private Integer numberOfCabins;

    /**
     * 舱位码
     */
    @Property(type = FieldType.Keyword)
    private String classCode;

    /**
     * 创建时间 （用于判断过期）
     */
    @Property(type = FieldType.Long)
    private Long createTime;

    /**
     * 存放 出发站 到达站 出发日期 航班号 （用于判断过期）
     */
    @Property(type = FieldType.Keyword)
    private String trafficInfo;
}

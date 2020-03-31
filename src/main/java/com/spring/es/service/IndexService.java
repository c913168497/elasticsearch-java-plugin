package com.spring.es.service;

import com.alibaba.fastjson.JSON;
import com.spring.es.config.es.ElasticClientDecorator;
import com.spring.es.model.entity.TrafficInfo;
import com.spring.es.plugin.EsBulkUtils;
import com.spring.es.plugin.EsUtils;
import com.spring.es.plugin.IndexBuilder;
import com.spring.es.utils.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 类描述：
 */
@Slf4j
@Service
public class IndexService {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    public static void main(String[] args) {
        String url = "es-cn-mp91ev3o900031dj7.public.elasticsearch.aliyuncs.com";
        Integer port = 9200;
        String userName = "elastic";
        String password = "44MzIIFV";
        RestHighLevelClient restHighLevelClient = new ElasticClientDecorator(new HttpHost(url, port), userName, password).getRestHighLevelClient();

        long currentTime = System.currentTimeMillis();
        long threeDay = 5 * 24 * 3600 * 1000;
        long startTime = currentTime - threeDay;
        // createTime  3 天前数据
        RangeQueryBuilder threeDaysAgo = EsUtils.rangeQuery(startTime, currentTime, "createTime");
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        boolBuilder.must(threeDaysAgo);
        SearchSourceBuilder searchSourceBuilder = EsUtils.listByQueryBuild(boolBuilder, null);
        SearchRequest searchRequest = EsUtils.getSearchRequest(searchSourceBuilder,  IndexBuilder.getIndexName(TrafficInfo.class), IndexBuilder.getType(TrafficInfo.class));
        SearchResponse searchResponse = EsUtils.getSearchResponse(restHighLevelClient, searchRequest);
        long total = searchResponse.getHits().getTotalHits();
        System.out.println("====" + total);
        // 14636058

    }

    /**
     * 注意 id 不要重复 重复会发生数据覆盖
     */
    public static void insertDataToIndex(RestHighLevelClient restHighLevelClient) {
        String value = "{\t\"DMK-KUL\": {\t\t\"2019-12-17\": {\t\t\t\"FD311\": [{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"},{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"},{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"}],\t\t\t\"AK891\": [{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"},{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"},{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"}],\t\t\t\"AK881\": [{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"},{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"},{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"}],\t\t\t\"AK883\": [{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"},{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"},{\"s\":\"1574231171\",\"p\":\"862.51\",\"c\":\"Z\"}]\t\t}\t}}";
        List<TrafficInfo> trafficInfos = CommonUtils.parsingJsonData(value);
        if (trafficInfos.size() == 0) {
            return;
        }

        List<IndexRequest> indexRequests = new ArrayList<>();
        for (TrafficInfo trafficInfo : trafficInfos) {
            IndexRequest indexRequest = new IndexRequest(IndexBuilder.getIndexName(TrafficInfo.class), IndexBuilder.getType(TrafficInfo.class));
            indexRequest.source(JSON.toJSONString(trafficInfo), XContentType.JSON);
            indexRequests.add(indexRequest);
        }
        // 批量新增
        EsBulkUtils.bulkRequest(restHighLevelClient, indexRequests, WriteRequest.RefreshPolicy.IMMEDIATE);
    }


    public void insert(List<TrafficInfo> trafficInfos) {
        long halfAnHour = 5 * 6 * 60 * 1000;
        String indexName = IndexBuilder.getIndexName(TrafficInfo.class);
        String typeName = IndexBuilder.getType(TrafficInfo.class);
        List<String> str = trafficInfos.stream().map(TrafficInfo::getTrafficInfo).collect(Collectors.toList());
        long currentTime = System.currentTimeMillis();
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        // 查询 出发站 到达站 出发日期 航班号 是否相同
        boolBuilder.must(QueryBuilders.termsQuery("trafficInfo", str));
        // 在当前半小时之内
        RangeQueryBuilder halfAnHourTime = EsUtils.rangeQuery(currentTime, halfAnHour, "createTime");
        boolBuilder.must(halfAnHourTime);
        SearchSourceBuilder searchSourceBuilder = EsUtils.listByQueryBuild(boolBuilder, null);
        SearchRequest searchRequest = EsUtils.getSearchRequest(searchSourceBuilder, indexName, typeName);
        SearchResponse searchResponse = EsUtils.getSearchResponse(restHighLevelClient, searchRequest);
        long total = searchResponse.getHits().getTotalHits();
        // 如果大于 0 则说明存在 出发站 到达站 出发日期 航班号 且 时间在半小时之内的数据
        if (total > 0) {
            updateTrafficTimeInfo(searchResponse, indexName, typeName, trafficInfos);
        }

        // 覆盖新增
//        bulkInsert(trafficInfos);
    }



        public void updateTrafficTimeInfo(SearchResponse searchResponse, String indexName, String typeName, List<TrafficInfo> newTrafficInfos) {
        IdsQueryBuilder idsQueryBuilder = QueryBuilders.idsQuery();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            String id = hit.getId();
            idsQueryBuilder.addIds(id);
        }

        SearchSourceBuilder searchSourceBuilder = EsUtils.listByQueryBuild(idsQueryBuilder, null);
        SearchRequest searchRequest = EsUtils.getSearchRequest(searchSourceBuilder, indexName, typeName);
        SearchResponse searchResponseIds = EsUtils.getSearchResponse(restHighLevelClient, searchRequest);
        List<TrafficInfo> trafficInfos = EsUtils.hitResultToObj(searchResponseIds.getHits());
        for (TrafficInfo trafficInfo : trafficInfos) {
            // 更新为当前时间
            trafficInfo.setDataTime(System.currentTimeMillis());
            newTrafficInfos.add(trafficInfo);
        }
    }


}

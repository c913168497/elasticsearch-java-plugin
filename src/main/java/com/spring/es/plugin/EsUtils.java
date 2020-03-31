package com.spring.es.plugin;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.spring.es.model.entity.Page;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 类描述：
 */
@Slf4j
public class EsUtils {
    /**
     *
     * @param queryBuilder
     * @param includes 表示只需要返回哪些参数 null 表示返回所有
     * @return
     */
    public static SearchSourceBuilder listByQueryBuild(QueryBuilder queryBuilder, String includes) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        if (StringUtils.isNotBlank(includes)) {
            sourceBuilder.fetchSource(includes.split(","), null);
        }

        return sourceBuilder;
    }

    /**
     * 搜索 公共调用
     * @param queryBuilder
     * @param pageNum
     * @param pageSize
     * @param includes
     * @return
     */
    public static SearchSourceBuilder pageByQueryBuild(QueryBuilder queryBuilder, int pageNum, int pageSize, String includes) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        if (Objects.nonNull(queryBuilder)) {
            sourceBuilder.query(queryBuilder);
        }

        sourceBuilder.from((pageNum - 1) * pageSize);
        sourceBuilder.size(pageSize);
        if (StringUtils.isNotBlank(includes)) {
            sourceBuilder.fetchSource(includes.trim().split(","), null);
        }

        return sourceBuilder;
    }

    /**
     * 排序字段
     *
     * @return
     */
    public static FieldSortBuilder sortField() {
        NestedSortBuilder nestedSortBuilder = new NestedSortBuilder("searchInfo");
        return SortBuilders.fieldSort("searchInfo.createTime").setNestedSort(nestedSortBuilder).order(SortOrder.DESC);
    }

    /**
     * 排序字段
     *
     * @param field
     * @return
     */
    public static FieldSortBuilder sortField(String field, SortOrder order) {
        FieldSortBuilder fsb = SortBuilders.fieldSort(field);
        fsb.order(order);
        return fsb;
    }

    /**
     * 排序字段
     *
     * @param field
     * @return
     */
    public static FieldSortBuilder includesField(String field) {
        FieldSortBuilder fsb = SortBuilders.fieldSort(field);
        fsb.order(SortOrder.DESC);
        return fsb;
    }

    /**
     *
     * @param restHighLevelClient
     * @param searchRequest
     * @return
     */
    public static SearchResponse getSearchResponse(RestHighLevelClient restHighLevelClient, SearchRequest searchRequest) {
        try {
            return restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            log.error("{}", e.getCause().getMessage());
            return new SearchResponse();
        }
    }

    /**
     * 设置索引和Type
     * @param sourceBuilder
     * @param indexName
     * @param typeName
     * @return
     */
    public static SearchRequest getSearchRequest(SearchSourceBuilder sourceBuilder, String indexName, String typeName) {
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.types(typeName);
        searchRequest.source(sourceBuilder);
        return searchRequest;
    }

    /**
     * 范围查询
     *
     * @param start
     * @param end
     * @param fileName
     */
    public static RangeQueryBuilder rangeQuery(Long start, Long end, String fileName) {
        RangeQueryBuilder rangBuilder = QueryBuilders.rangeQuery(fileName);
        if (Objects.nonNull(start)) {
            rangBuilder.gte(start);
        }

        if (Objects.nonNull(end)) {
            rangBuilder.lte(end);
        }

        return rangBuilder;
    }

    public static String getResultDataIndexOne(SearchHits shs) {
        if (Objects.isNull(shs) || shs.totalHits == 0) {
            return "{}";
        }

        return shs.getHits()[0].getSourceAsString();
    }

    public static String getResultData(SearchHits shs) {
        if (Objects.isNull(shs) || shs.totalHits == 0) {
            return "[]";
        }

        return "[".concat(Arrays.stream(shs.getHits()).map(SearchHit::getSourceAsString).collect(Collectors.joining(","))).concat("]");
    }

    /**
     * 结果集转化为 实体
     *
     * @param shs
     * @return
     */
    public static <T> List<T> hitResultToObj(SearchHits shs) {
        List<T> resultData = new ArrayList<>();
        if (Objects.isNull(shs) || shs.totalHits == 0) {
            return new ArrayList<>();
        }

        for (SearchHit hit : shs) {
            T object = JSONObject.parseObject(hit.getSourceAsString(), new TypeReference<T>(){});
            resultData.add(object);
        }

        return resultData;
    }

    /**
     * 分页结果集转换
     *
     * @param shs
     * @param pageNum
     * @param pageSize
     * @param <T>
     * @return
     */
    public static <T> Page<T> toPageResult(SearchHits shs, int pageNum, int pageSize, List<T> resultData) {
        Page<T> page = new Page<>();
        page.setPageNum(pageNum);
        page.setPageSize(pageSize);
        page.setContent(Objects.nonNull(resultData) ? resultData : new ArrayList<>());
        page.setTotal(shs.getTotalHits());
        return page;
    }
}

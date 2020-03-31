package com.spring.es.plugin;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.spring.es.config.es.ElasticClientDecorator;
import com.spring.es.model.entity.TrafficInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.client.utils.DateUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 类描述：批量操作类
 */
@Slf4j
public class EsBulkUtils {

    private EsBulkUtils() {
    }

    /**
     * 批量新增
     *
     * @param client
     * @param indexRequests
     * @param refreshPolicy WAIT_UNTIL (等待自动刷新)
     * @return
     */
    public static boolean bulkRequest(RestHighLevelClient client, List<IndexRequest> indexRequests, WriteRequest.RefreshPolicy refreshPolicy) {
        BulkRequest bulkRequest = getBulkRequest(indexRequests, refreshPolicy);
        return bulkData(bulkRequest, client);
    }

    /**
     * 批量新增 (异步)
     *
     * @param client
     * @param indexRequests
     * @param refreshPolicy WAIT_UNTIL (等待自动刷新)
     * @return
     */
    public static void asyncBulkRequest(RestHighLevelClient client, List<IndexRequest> indexRequests, WriteRequest.RefreshPolicy refreshPolicy) {
        BulkRequest bulkRequest = getBulkRequest(indexRequests, refreshPolicy);
        asyncBulkData(bulkRequest, client);
    }

    private static BulkRequest getBulkRequest(List<IndexRequest> indexRequests, WriteRequest.RefreshPolicy refreshPolicy) {
        BulkRequest request = new BulkRequest();
        indexRequests.forEach(request::add);
        request.setRefreshPolicy(refreshPolicy);
        return request;
    }

    /**
     * 检查结果集是否有效
     *
     * @param t
     * @param function1
     * @param <T>
     * @param <R>
     * @return
     */
    public static <T, R> String checkSearchValid(T t, Function<T, R> function1) {
        R r = function1.apply(t);
        if (Objects.nonNull(r)) {
            log.error(JSONObject.toJSONString(r));
            return JSONObject.toJSONString(r);
        }

        return "";
    }

    /**
     * 批量处理结果 返回
     *
     * @param request
     * @param client
     */
    private static boolean bulkData(BulkRequest request, RestHighLevelClient client) {
        try {
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            log.info("此次新增条数: {}", bulkResponse.getItems().length);
            for (BulkItemResponse bulkItemResponse : bulkResponse) {

                if (Objects.nonNull(bulkItemResponse.getFailure())) {
                    log.error("新增错误：" + JSONObject.toJSONString(bulkItemResponse));
                    continue;
                }

                DocWriteResponse itemResponse = bulkItemResponse.getResponse();
                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                }

            }
        } catch (IOException e) {
            log.error(e.getMessage());
            return false;
        }

        return true;
    }

    private static void asyncBulkData(BulkRequest request, RestHighLevelClient client) {
        client.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {

            }

            @Override
            public void onFailure(Exception e) {
                log.error("新增错误：" + JSONObject.toJSONString(e.getCause().getMessage()));
            }
        });
    }

    /**
     * 异步删除带监听     * @param client
     */
    public static void deleteByQueryAsync(RestHighLevelClient client, String indexName, String typeName) {
        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest(indexName);
        BoolQueryBuilder boolBuilder = QueryBuilders.boolQuery();
        long currentTime = System.currentTimeMillis();
        long threeDay = 5 * 24 * 3600 * 1000;
        long startTime = currentTime - threeDay;
        // createTime  3 天前数据
        RangeQueryBuilder threeDaysAgo = EsUtils.rangeQuery(startTime, currentTime, "createTime");
        boolBuilder.must(threeDaysAgo);
        deleteByQueryRequest.setQuery(boolBuilder);
        deleteByQueryRequest.types(typeName);
        deleteByQueryRequest.setRefresh(true);
        client.deleteByQueryAsync(deleteByQueryRequest, RequestOptions.DEFAULT, new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse bulkItemResponses) {
                        System.out.println(JSON.toJSONString(bulkItemResponses.getStatus()));
                        //log.info(JSON.toJSONString(bulkItemResponses.getStatus()));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error("删除：" + JSONObject.toJSONString(e));
                    }
                }
        );
    }

    public static void main(String[] args) {
        String url = "es-cn-mp91ev3o900031dj7.public.elasticsearch.aliyuncs.com";
        Integer port = 9200;
        String userName = "elastic";
        String password = "44MzIIFV";
        // 13212313
        RestHighLevelClient restHighLevelClient = new ElasticClientDecorator(new HttpHost(url, port), userName, password).getRestHighLevelClient();
        EsBulkUtils.deleteByQueryAsync(restHighLevelClient, IndexBuilder.getIndexName(TrafficInfo.class), IndexBuilder.getType(TrafficInfo.class));
    }

}

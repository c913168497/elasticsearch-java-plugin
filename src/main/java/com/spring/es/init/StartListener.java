package com.spring.es.init;

import com.spring.es.model.entity.TrafficInfo;
import com.spring.es.plugin.IndexBuilder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.IOException;


/**
 * 类描述：启动初始化 索引
 */
@Slf4j
@Component
public class StartListener implements ApplicationRunner {

    private final RestHighLevelClient restHighLevelClient;

    public StartListener(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * 会在服务启动完成后立即执行
     */
    @Override
    public void run(ApplicationArguments args) {
        // 初始化索引 和 type */
        try {
            if (!IndexBuilder.isIndexExists(restHighLevelClient, IndexBuilder.getIndexName(TrafficInfo.class))) {
                log.info("初始化航班索引: {}", IndexBuilder.getIndexName(TrafficInfo.class));
                IndexBuilder.createIndex(restHighLevelClient, TrafficInfo.class, IndexBuilder.getIndexName(TrafficInfo.class));
            }
        } catch (IOException e) {
            log.error("创建索引失败");
        }
    }

}



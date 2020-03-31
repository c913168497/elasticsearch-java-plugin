package com.spring.es.config.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;


/**
 * 类描述：ES 基础配置
 */
@Configuration
@EnableConfigurationProperties(ElasticsProperties.class)
public class ElasticsConfig {

    @Autowired
    private ElasticsProperties elasticsProperties;

    /**
     * 初始化
     */
    @Bean
    public RestHighLevelClient restHighLevelClient() {
        return getEsClientDecorator().getRestHighLevelClient();
    }

    @Bean
    @Scope("singleton")
    public ElasticClientDecorator getEsClientDecorator() {
        return new ElasticClientDecorator(new HttpHost(elasticsProperties.getClusterNodes(), elasticsProperties.getPort()), elasticsProperties.getUserName(), elasticsProperties.getPassword());
    }


}

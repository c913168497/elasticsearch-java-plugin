package com.spring.es.config.es;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.Objects;

/**
 * 类描述：Es http 初始化
 */

public class ElasticClientDecorator {

    private RestHighLevelClient restHighLevelClient;
    private RestClientBuilder builder;
    private HttpHost httpHost;
    private String userName;
    private String password;

    public ElasticClientDecorator(HttpHost httpHost, String userName, String password) {
        this.httpHost = httpHost;
        this.userName = userName;
        this.password = password;
    }

    public RestHighLevelClient getRestHighLevelClient() {
        if (Objects.nonNull(restHighLevelClient)) {
            return restHighLevelClient;
        }

        if (StringUtils.isBlank(userName) || StringUtils.isBlank(password)) {
            builder = RestClient.builder(httpHost);
        } else {
            setBuilder();
        }

        restHighLevelClient = new RestHighLevelClient(builder);
        return restHighLevelClient;
    }

    private void setBuilder() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(userName, password));
        builder = RestClient.builder(httpHost).
                setHttpClientConfigCallback(httpClientBuilder ->
                        httpClientBuilder.setDefaultCredentialsProvider(
                                credentialsProvider));
    }
}

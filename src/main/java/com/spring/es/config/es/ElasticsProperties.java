package com.spring.es.config.es;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 类描述：ES 配置类
 */
@ConfigurationProperties(prefix = "spring.data.elasticsearch")
@Data
public class ElasticsProperties {

    /**
     * 名称
     */
    private String clusterName;

    /**
     * 节点
     */
    private String clusterNodes;

    /**
     * 端口号
     */
    private Integer port;

    /**
     * 用户名
     */
    private String userName;

    /**
     * 密码
     */
    private String password;


}

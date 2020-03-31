package com.spring.es.plugin.annotations;


import java.lang.annotation.*;

/**
 * 类描述：Document
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Document {

    /**
     * 索引名称
     */
    String indexName() default "";

    /**
     * 索引类型
     */
    String type() default "";

    /**
     * 分片数
     */
    short shards() default 5;

    /**
     * 副本数
     */
    short replicas() default 1;

    /**
     * 刷新间隔
     */
    String refreshInterval() default "1s";
}

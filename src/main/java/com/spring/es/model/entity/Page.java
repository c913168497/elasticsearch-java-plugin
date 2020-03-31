package com.spring.es.model.entity;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * 类描述：
 */

@Getter
@Setter
public class Page<T> {

    private int pageNum;

    private int pageSize;

    private List<T> content;

    private long total;
}

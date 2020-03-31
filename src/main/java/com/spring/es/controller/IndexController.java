package com.spring.es.controller;

import com.spring.es.service.IndexService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 类描述：

 */

@RestController(value = "indexController")
@RequestMapping("/api/index/option")
public class IndexController {

    @Autowired
    private IndexService indexService;

    @GetMapping
    private String initData() {
//        indexService.insertDataToIndex();
        return "成功";
    }

}

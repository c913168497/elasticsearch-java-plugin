/*
 * Copyright 2014-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.spring.es.plugin;

import com.spring.es.plugin.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.core.ResolvableType;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * 类描述：ES 生成索引 和 type 插件
 */
public class IndexBuilder {

    public static final String FIELD_DATA = "fielddata";
    public static final String FIELD_STORE = "store";
    public static final String FIELD_TYPE = "type";
    public static final String FIELD_INDEX = "index";
    public static final String FIELD_FORMAT = "format";
    public static final String FIELD_SEARCH_ANALYZER = "search_analyzer";
    public static final String FIELD_INDEX_ANALYZER = "analyzer";
    public static final String FIELD_NORMALIZER = "normalizer";
    public static final String FIELD_PROPERTIES = "properties";
    public static final String FIELD_COPY_TO = "copy_to";

    public static final String TYPE_VALUE_KEYWORD = "keyword";

    /**
     * WriteRequest.RefreshPolicy.WAIT_UNTIL : 一直保持请求连接中，直接当所做的更改对于搜索查询可见时的刷新发生后，再将结果返回 (就是等待自动刷新)
     * WriteRequest.RefreshPolicy.IMMEDIATE ： 立即刷新整个 index  比较消耗性能，当超过 1000 万数据时 会存在一定数据延迟现象
     * WriteRequest.RefreshPolicy.NONE ： 不刷新索引，不推荐，无法查询到新增的数据
     *
     * @param clazz
     * @return
     * @throws IOException
     */
    private static XContentBuilder putMapping(Class clazz) throws IOException {
        Document document = getDocument(clazz);

        String indexType = document.type();
        String idFieldName = getIdFieldName(clazz);
        return buildMapping(clazz, indexType, idFieldName);
    }


    /**
     * 创建索引
     *
     * @param client
     * @param clazz
     * @throws IOException
     */
    public static void createIndex(RestHighLevelClient client, Class clazz) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(getIndexName(clazz));
        request.settings(putSettings(clazz), XContentType.JSON);
        request.mapping(getType(clazz), putMapping(clazz));
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 创建索引
     *
     * @param client
     * @param clazz
     * @throws IOException
     */
    public static void createIndex(RestHighLevelClient client, Class clazz, String indexName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(putSettings(clazz), XContentType.JSON);
        request.mapping(getType(clazz), putMapping(clazz));
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 判断索引存在
     *
     * @param client
     * @return
     * @throws IOException
     */
    public static boolean isIndexExists(RestHighLevelClient client, Class clazz) throws IOException {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(getIndexName(clazz));
        request.local(false);
        request.humanReadable(true);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 判断索引存在
     *
     * @param client
     * @return
     * @throws IOException
     */
    public static boolean isIndexExists(RestHighLevelClient client, String indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest();
        request.indices(indexName);
        request.local(false);
        request.humanReadable(true);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    /**
     * 创建 Settings
     *
     * @param clazz
     * @return
     */
    private static String putSettings(Class clazz) {
        Annotation annotation = clazz.getAnnotation(Document.class);
        if (Objects.isNull(annotation)) {
            return null;
        }
        Document document = (Document) annotation;
        /** 设置节点刷新间隔 默认 1 秒*/
        String refreshInterval = document.refreshInterval();
        /** 设置副本数 */
        short replicas = document.replicas();
        /** 设置分片数 */
        short shards = document.shards();

        return getSetting(refreshInterval, replicas, shards);
    }

    /**
     * 近义词配置地址
     * /usr/share/elasticsearch/config/analysis-ik/synonym.txt
     * <p>
     * 这里采用分词器 ng
     *
     * @param refreshInterval
     * @param replicas
     * @param shards
     * @return -v /opt/analysis-ik:/usr/share/elasticsearch/config/analysis-ik
     */
    public static String getSetting(String refreshInterval, short replicas, short shards) {
        String setting = "{\n" +
                "    \"index.refresh_interval\": \"#{interval}\",\n" +
                "    \"index.number_of_replicas\": #{replicas},\n" +
                "    \"index.number_of_shards\": #{shards},\n" +
                "    \"index.max_result_window\": 100000000,\n" +
                "     \"analysis\": {" +
                "      \"analyzer\": { " +
                "        \"charSplit\": {" +
                "        \"type\": \"custom\"," +
                "        \"tokenizer\": \"ngram_tokenizer\"" +
                "    }" +
                "   }," +
                " \"tokenizer\": {" +
                "           \"ngram_tokenizer\": {" +
                "             \"type\": \"ngram\"," +
                "             \"min_gram\": \"1\"," +
                "             \"max_gram\": \"30\"," +
                "             \"token_chars\": [" +
                "              \"letter\"," +
                "              \"digit\"" +
                "             ]" +
                "           }" +
                "        }" +
                "      }" +
                "}";
        setting = setting.replace("#{interval}", refreshInterval);
        setting = setting.replace("#{replicas}", String.valueOf(replicas));
        setting = setting.replace("#{shards}", String.valueOf(shards));
        return setting;
    }


    /**
     * 获取 document 注解
     *
     * @param clazz
     * @return
     */
    private static Document getDocument(Class clazz) throws IOException {
        Annotation annotation = clazz.getAnnotation(Document.class);
        if (Objects.isNull(annotation)) {
            throw new IOException("无法为该类设置 Mapping , 请先设置好 Document 注解");
        }

        return (Document) annotation;
    }

    /**
     * 索引 type
     *
     * @param clazz
     * @return
     */
    public static String getType(Class clazz) {
        Document document = null;
        try {
            document = getDocument(clazz);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return document.type();
    }

    /**
     * 索引名
     *
     * @param clazz
     * @return
     */
    public static String getIndexName(Class clazz) {
        Document document = null;
        try {
            document = getDocument(clazz);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return document.indexName();
    }

    /**
     * 获取 id 属性
     *
     * @param clazz
     * @return
     */
    private static String getIdFieldName(Class clazz) {
        Field[] field02 = clazz.getDeclaredFields();
        for (Field field : field02) {
            Id id = field.getAnnotation(Id.class);
            if (Objects.nonNull(id)) {
                return field.getName();
            }

        }

        return "";
    }

    /**
     * 通过 class 获取 XContentBuilder 类
     *
     * @param clazz
     * @param indexType
     * @param idFieldName
     * @return
     * @throws IOException
     */
    private static XContentBuilder buildMapping(Class clazz, String indexType, String idFieldName) throws IOException {

        XContentBuilder mapping = jsonBuilder().startObject().startObject(indexType);
        // Properties
        XContentBuilder xContentBuilder = mapping.startObject(FIELD_PROPERTIES);

        mapEntity(xContentBuilder, clazz, true, idFieldName, "", false, FieldType.Auto, null);

        return xContentBuilder.endObject().endObject().endObject();
    }

    /**
     * 检查属性 是否符合要求
     *
     * @param fields
     * @param nestedOrObjectField
     * @return
     */
    private static boolean checkPropertyField(Field[] fields, boolean nestedOrObjectField) {
        return (isAnyPropertyAnnotatedAsField(fields) || nestedOrObjectField);
    }

    /**
     * 根据 Class 对象 构建 XContentBuilder
     *
     * @param xContentBuilder
     * @param clazz                 目标类
     * @param isRootObject          是否是根对象
     * @param idFieldName           id
     * @param nestedObjectFieldName 属性名
     * @param nestedOrObjectField   是否是内嵌对象
     * @param fieldType             属性类型
     * @param fieldAnnotation       属性注解
     * @throws IOException
     */
    private static void mapEntity(XContentBuilder xContentBuilder, Class clazz, boolean isRootObject, String idFieldName,
                                  String nestedObjectFieldName, boolean nestedOrObjectField, FieldType fieldType, Property fieldAnnotation) throws IOException {
        /** 获取类 属性 数组 */
        Field[] fields = retrieveFields(clazz);

        if (!isRootObject && checkPropertyField(fields, nestedOrObjectField)) {
            String type = FieldType.Object.toString().toLowerCase();

            if (nestedOrObjectField) {
                type = fieldType.toString().toLowerCase();
            }

            XContentBuilder t = xContentBuilder.startObject(nestedObjectFieldName).field(FIELD_TYPE, type);
            /** 判断内嵌属性 */
            if (nestedOrObjectField && FieldType.Nested == fieldType && fieldAnnotation.includeInParent()) {
                t.field("include_in_parent", fieldAnnotation.includeInParent());
            }
            t.startObject(FIELD_PROPERTIES);
        }

        for (Field field : fields) {
            Property singleField = field.getAnnotation(Property.class);
            if (isAnnotated(field)) {
                if (singleField == null) {
                    continue;
                }
                boolean nestedOrObject = isNestedOrObjectField(field);
                mapEntity(xContentBuilder, getFieldType(field), false, "", field.getName(), nestedOrObject, singleField.type(), field.getAnnotation(Property.class));
                if (nestedOrObject) {
                    continue;
                }
            }

            MultiField multiField = field.getAnnotation(MultiField.class);

            if (isRootObject && singleField != null && isIdField(field, idFieldName)) {
                applyDefaultIdFieldMapping(xContentBuilder, field);
            } else if (multiField != null) {
                addMultiFieldMapping(xContentBuilder, field, multiField, isNestedOrObjectField(field));
            } else if (singleField != null) {
                addSingleFieldMapping(xContentBuilder, field, singleField, isNestedOrObjectField(field));
            }
        }

        if (!isRootObject && isAnyPropertyAnnotatedAsField(fields) || nestedOrObjectField) {
            xContentBuilder.endObject().endObject();
        }
    }

    /**
     * 获取类 属性 数组
     *
     * @param clazz
     * @return
     */
    private static Field[] retrieveFields(Class clazz) {
        //
        List<Field> fields = new ArrayList<>();

        // Keep backing up the inheritance hierarchy.
        Class targetClass = clazz;
        do {
            fields.addAll(Arrays.asList(targetClass.getDeclaredFields()));
            targetClass = targetClass.getSuperclass();
        }
        while (targetClass != null && targetClass != Object.class);

        return fields.toArray(new Field[fields.size()]);
    }

    /**
     * 判断是否 是符合的 注解
     *
     * @param field
     * @return
     */
    private static boolean isAnnotated(Field field) {
        return Objects.nonNull(field.getAnnotation(Property.class)) ||
                Objects.nonNull(field.getAnnotation(MultiField.class));
    }

    /**
     * 应用默认 ID 属性映射
     *
     * @param xContentBuilder
     * @param field
     * @throws IOException
     */
    private static void applyDefaultIdFieldMapping(XContentBuilder xContentBuilder, Field field)
            throws IOException {
        xContentBuilder.startObject(field.getName())
                .field(FIELD_TYPE, TYPE_VALUE_KEYWORD)
                .field(FIELD_INDEX, true);
        xContentBuilder.endObject();
    }

    /**
     * 增加属性 单类型 映射
     *
     * @throws IOException
     */
    private static void addSingleFieldMapping(XContentBuilder builder, Field field, Property annotation, boolean nestedOrObjectField) throws IOException {
        builder.startObject(field.getName());
        addFieldMappingParameters(builder, annotation, nestedOrObjectField);
        builder.endObject();
    }

    /**
     * 增加属性 多类型 映射
     *
     * @throws IOException
     */
    private static void addMultiFieldMapping(
            XContentBuilder builder,
            Field field,
            MultiField annotation,
            boolean nestedOrObjectField) throws IOException {

        // main field
        builder.startObject(field.getName());
        addFieldMappingParameters(builder, annotation.mainField(), nestedOrObjectField);

        // inner fields
        builder.startObject("fields");
        for (InnerField innerField : annotation.otherFields()) {
            builder.startObject(innerField.suffix());
            addFieldMappingParameters(builder, innerField, false);
            builder.endObject();
        }
        builder.endObject();

        builder.endObject();
    }

    /**
     * 属性 参数映射方法
     *
     * @param builder
     * @param annotation
     * @param nestedOrObjectField
     * @throws IOException
     */
    private static void addFieldMappingParameters(XContentBuilder builder, Object annotation, boolean nestedOrObjectField) throws IOException {
        boolean index = true;
        boolean store = false;
        boolean fielddata = false;
        FieldType type = null;
        DateFormat dateFormat = null;
        String datePattern = null;
        String analyzer = null;
        String searchAnalyzer = null;
        String normalizer = null;
        String[] copyTo = null;

        if (annotation instanceof Property) {
            // @Property
            Property fieldAnnotation = (Property) annotation;
            index = fieldAnnotation.index();
            store = fieldAnnotation.store();
            fielddata = fieldAnnotation.fielddata();
            type = fieldAnnotation.type();
            dateFormat = fieldAnnotation.format();
            datePattern = fieldAnnotation.pattern();
            analyzer = fieldAnnotation.analyzer();
            searchAnalyzer = fieldAnnotation.searchAnalyzer();
            normalizer = fieldAnnotation.normalizer();
            copyTo = fieldAnnotation.copyTo();
        } else if (annotation instanceof InnerField) {
            // @InnerField
            InnerField fieldAnnotation = (InnerField) annotation;
            index = fieldAnnotation.index();
            store = fieldAnnotation.store();
            fielddata = fieldAnnotation.fielddata();
            type = fieldAnnotation.type();
            dateFormat = fieldAnnotation.format();
            datePattern = fieldAnnotation.pattern();
            analyzer = fieldAnnotation.analyzer();
            searchAnalyzer = fieldAnnotation.searchAnalyzer();
            normalizer = fieldAnnotation.normalizer();
        } else {
            throw new IllegalArgumentException("annotation must be an instance of @Property or @InnerField");
        }

        if (!nestedOrObjectField) {
            builder.field(FIELD_STORE, store);
        }
        if (fielddata) {
            builder.field(FIELD_DATA, fielddata);
        }
        if (type != FieldType.Auto) {
            builder.field(FIELD_TYPE, type.name().toLowerCase());

            if (type == FieldType.Date && dateFormat != DateFormat.none) {
                builder.field(FIELD_FORMAT, dateFormat == DateFormat.custom ? datePattern : dateFormat.toString());
            }
        }
        if (!index) {
            builder.field(FIELD_INDEX, index);
        }
        if (!StringUtils.isEmpty(analyzer)) {
            builder.field(FIELD_INDEX_ANALYZER, analyzer);
        }
        if (!StringUtils.isEmpty(searchAnalyzer)) {
            builder.field(FIELD_SEARCH_ANALYZER, searchAnalyzer);
        }
        if (!StringUtils.isEmpty(normalizer)) {
            builder.field(FIELD_NORMALIZER, normalizer);
        }
        if (copyTo != null && copyTo.length > 0) {
            builder.field(FIELD_COPY_TO, copyTo);
        }
    }

    /**
     * 通过 ResolvableType 处理泛型
     *
     * @param field
     * @return https://www.jianshu.com/p/456b1b6ea51f 不理解可以看这篇文章
     */
    protected static Class<?> getFieldType(Field field) {
        return field.getClass();
    }

    /**
     * 是否包含 Property 注解
     *
     * @param fields
     * @return
     */
    private static boolean isAnyPropertyAnnotatedAsField(Field[] fields) {
        if (fields != null) {
            for (Field field : fields) {
                if (field.isAnnotationPresent(Property.class)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 是否是 ID 属性
     *
     * @param field
     * @param idFieldName
     * @return
     */
    private static boolean isIdField(Field field, String idFieldName) {
        return idFieldName.equals(field.getName());
    }

    /**
     * 是否是 查询忽略 属性
     *
     * @param field
     * @param parentFieldAnnotation
     * @return
     */
    private static boolean isInIgnoreFields(Field field, Property parentFieldAnnotation) {
        if (Objects.nonNull(parentFieldAnnotation)) {
            String[] ignoreFields = parentFieldAnnotation.ignoreFields();
            return Arrays.asList(ignoreFields).contains(field.getName());
        }
        return false;
    }

    /**
     * 是否是 内嵌 属性
     *
     * @param field
     * @return
     */
    private static boolean isNestedOrObjectField(Field field) {
        Property fieldAnnotation = field.getAnnotation(Property.class);
        return Objects.nonNull(fieldAnnotation) && (FieldType.Nested == fieldAnnotation.type() || FieldType.Object == fieldAnnotation.type());
    }

}

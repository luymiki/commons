/*
 * Copyright 2017 com.anluy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.anluy.commons.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.anluy.commons.Configuration;
import com.anluy.commons.ElasticsearchException;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 功能说明：Elasticsearch restful 封装类
 * <p>
 * Created by hc.zeng on 2017/10/20.
 */
public class ElasticsearchRestClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchRestClient.class);

    private final RestClient restClient;

    public ElasticsearchRestClient(String host, int port, String userName, String password) {
        this.restClient = this.createRestClient(host, port, userName, password);
    }

    private RestClient createRestClient(String host, int port, String userName, String password) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"));
        Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};
        builder.setDefaultHeaders(defaultHeaders);
        builder.setMaxRetryTimeoutMillis(100000);
        builder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(HttpHost host) {

            }
        });
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setSocketTimeout(60000).setConnectTimeout(50000);
            }
        });
        return builder.build();
    }


    /**
     * 发送请求并返回请求结果
     *
     * @param method
     * @param endpoint
     * @param params
     * @param entity
     * @param headers
     * @return
     */
    private String parser(String method, String endpoint, Map<String, String> params, HttpEntity entity, Header[] headers) {
        try {
            Response response = restClient.performRequest(method, endpoint, params, entity, headers);
            return this.parser(response);
        } catch (IOException e) {
            throw new ElasticsearchException("操作Elasticsearch失败", e);
        }
    }

    /**
     * 读取返回字符串
     *
     * @param response
     * @return
     */
    private String parser(Response response) {
        try {
            StringBuffer sb = new StringBuffer();
            if (response.getStatusLine() != null && response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                BufferedReader bfi = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                String line = null;
                while ((line = bfi.readLine()) != null) {
                    sb.append(line);
                }
                return sb.toString();
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new ElasticsearchException("读取Elasticsearch返回数据失败", e);
        }
    }

    /**
     * 保存数据
     *
     * @param data      数据MAP
     * @param id        数据的id
     * @param indexName 保存到es的index名称
     * @throws ElasticsearchException
     */
    public void save(Map<String, Object> data, String id, String indexName) {
        Assert.hasText(indexName, "indexName 不能为空！");
        Assert.hasText(id, "id 不能为空！");
        Assert.notEmpty(data, "data 不能为空！");

        Header[] headers = new Header[]{};
        StringBuffer builder = new StringBuffer();

//        String endpoint = "/" + indexName + "/doc/_bulk";
//        Map<String, String> params = Collections.<String, String>emptyMap();
//        builder.append("{\"index\":{\"_id\":\"").append(id).append("\"");
//        builder.append("}}").append("\n");

        String endpoint = "/" + indexName + "/doc/" + id;
        if (data.containsKey("_parent")) {//save child document
            endpoint += "?parent=" + data.get("_parent");
            data.remove("_parent");
        }
        builder.append(JSON.toJSONString(data, new SerializerFeature[]{SerializerFeature.WriteMapNullValue})).append("\n");
        try {
            HttpEntity entity = new NStringEntity(builder.toString(), ContentType.APPLICATION_JSON);
            Response response = restClient.performRequest("POST", endpoint, Collections.emptyMap(), entity);
            if (response != null && response.getStatusLine() != null) {
                int httpStatus = response.getStatusLine().getStatusCode();
                if (httpStatus == HttpStatus.SC_OK || httpStatus == HttpStatus.SC_CREATED) {
                    refresh(indexName); //刷新索引
                    return;
                }
            }
            throw new ElasticsearchException("保存文档方法执行失败");
        } catch (Exception e) {
            throw new ElasticsearchException("返回结果解析异常", e);
        }

    }

    /**
     * 更新文档数据
     * 注：更新child文档时，在recordData(HashMap)中放入元数据_parent
     *
     * @param recordData 文档记录
     * @param indexName  索引名
     * @param id         文档主键ID值
     * @return 返回请求响应的状态
     */
    public void update(Map recordData, String id, String indexName) {
        Assert.hasText(indexName, "indexName 不能为空！");
        Assert.hasText(id, "id 不能为空！");
        Assert.notEmpty(recordData, "recordData 不能为空！");
        try {
            String url = "/" + indexName + "/doc/" + id + "/_update";
            if (recordData.containsKey("_parent")) {//save child document
                url += "?parent=" + recordData.get("_parent");
                recordData.remove("_parent");
            }
            String doc = "{\"doc\":" + JSON.toJSONString(recordData, SerializerFeature.WriteMapNullValue) + "}";
            HttpEntity httpEntity = new StringEntity(doc, ContentType.APPLICATION_JSON);
            Response response = restClient.performRequest("POST", url, Collections.emptyMap(), httpEntity);
            if (response != null && response.getStatusLine() != null) {
                int httpStatus = response.getStatusLine().getStatusCode();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("\nupdate operation done. HttpStatus:" + httpStatus);
                }
                if (httpStatus == HttpStatus.SC_OK || httpStatus == HttpStatus.SC_CREATED) {
                    refresh(indexName); //刷新索引
                    return;
                }
            }
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("数据保存失败");
            }
            throw new ElasticsearchException("更新文档方法执行失败");
        } catch (Exception e) {
            LOGGER.error("update方法执行异常！", e);
            throw new ElasticsearchException("更新文档方法执行失败");
        }
    }

    /**
     * 根据文档ID删除该条记录
     *
     * @param indexName
     * @param id
     * @return
     */
    public void remove(String id, String indexName) {
        Assert.hasText(indexName, "indexName 不能为空！");
        Assert.hasText(id, "id 不能为空！");
        try {
            String url = "/" + indexName + "/doc/" + id;
            Response response = restClient.performRequest("DELETE", url);
            String resultStr = this.parser(response);

            if (resultStr != null) {
                Configuration configuration = Configuration.from(resultStr);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("\nremove operation done. ResponseData:\n" + resultStr);
                }
                int found = configuration.getInt("_shards.successful");
                if (found == 1) {
                    refresh(indexName);
                }
            } else {
                throw new ElasticsearchException("删除文档方法执行失败");
            }
        } catch (Exception e) {
            LOGGER.error("remove方法执行异常！", e);
            throw new ElasticsearchException("删除文档方法执行失败");
        }
    }

    /**
     * 根据查询结果删除记录
     *
     * @param indexName
     * @param queryDsl
     * @return
     */
    public void deleteByQuery(String queryDsl, String indexName) {
        Assert.hasText(indexName, "indexName 不能为空！");
        Assert.hasText(queryDsl, "queryDsl 不能为空！");
        try {
            String url = "/" + indexName + "/doc/_delete_by_query";
            HttpEntity entity = new NStringEntity(queryDsl, ContentType.APPLICATION_JSON);
            Response response = restClient.performRequest("POST", url, Collections.emptyMap(), entity);
            if (response != null && response.getStatusLine() != null) {
                int httpStatus = response.getStatusLine().getStatusCode();
                if (httpStatus == HttpStatus.SC_OK || httpStatus == HttpStatus.SC_CREATED) {
                    refresh(indexName); //刷新索引
                    return;
                }
            }
            throw new ElasticsearchException("删除文档方法执行失败");

        } catch (Exception e) {
            LOGGER.error("deleteByQuery方法执行异常！", e);
            throw new ElasticsearchException("删除文档方法执行失败");
        }
    }

    /**
     * 根据ID获取对应的文档记录
     *
     * @param indexName     索引名
     * @param id            文档主键ID值
     * @param includeFields 需要返还的字段，多个用逗号分隔，字段名支持通配符
     * @param excludeFields 不需要返还的字段，多个用逗号分隔，字段名支持通配符
     *                      includeFields和excludeFields都为空时默认返还所有字段
     * @return
     * @throws IOException
     */
    public Map get(String indexName, String id, String includeFields, String excludeFields) {
        Assert.hasText(indexName, "indexName 不能为空！");
        Assert.hasText(id, "id 不能为空！");

        String url = "/" + indexName + "/doc/" + id;
        if (StringUtils.isNotBlank(includeFields) && !StringUtils.isNotBlank(excludeFields)) {
            url += "?_source_include=" + includeFields + "&_source_exclude=" + excludeFields;
        } else if (StringUtils.isNotBlank(includeFields)) {
            url += "?_source_include=" + includeFields;
        } else if (StringUtils.isNotBlank(excludeFields)) {
            url += "?_source_exclude=" + excludeFields;
        }
        try {
            Response response = restClient.performRequest("GET", url);
            String resultStr = this.parser(response);
            JSONObject rj = JSONObject.parseObject(resultStr);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("\nget done. ResponseData:\n" + resultStr);
            }
            if (rj != null) {
                return (Map) rj.get("_source");
            }
        } catch (Exception e) {
            LOGGER.error("查找失败！", e);
            throw new ElasticsearchException("查找失败！", e);
        }
        return null;
    }

    /**
     * 根据ID列表获取对应的文档记录
     *
     * @param indexName     索引名
     * @param idList        文档主键ID值列表
     * @param includeFields 需要返还的字段，多个用逗号分隔，字段名支持通配符
     * @param excludeFields 不需要返还的字段，多个用逗号分隔，字段名支持通配符
     *                      includeFields和excludeFields都为空时默认返还所有字段
     * @return
     * @throws IOException
     */
    public List<Map> get(String indexName, List<String> idList, String includeFields, String excludeFields) {
        Assert.hasText(indexName, "indexName 不能为空！");
        Assert.notEmpty(idList, "idList 不能为空！");
        List<Map> successGetItems = new ArrayList<>();
        try {
            String url = "/" + indexName + "/doc/_mget";
            if (!Strings.isNullOrEmpty(includeFields) && !Strings.isNullOrEmpty(excludeFields)) {
                url += "?_source_include=" + includeFields + "&_source_exclude=" + excludeFields;
            } else if (!Strings.isNullOrEmpty(includeFields)) {
                url += "?_source_include=" + includeFields;
            } else if (!Strings.isNullOrEmpty(excludeFields)) {
                url += "?_source_exclude=" + excludeFields;
            }
            StringBuilder builder = new StringBuilder("{\"ids\":[");
            IntStream.range(0, idList.size()).forEach(i -> {
                if (i == 0) {
                    builder.append("\"").append(idList.get(i)).append("\"");
                } else {
                    builder.append(",\"").append(idList.get(i)).append("\"");
                }
            });
            builder.append("]}");
            List<String> notFoundIds = new ArrayList<>();
            HttpEntity httpEntity = new StringEntity(builder.toString(), ContentType.APPLICATION_JSON);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("\n" + builder.toString());
            }
            String resultStr = this.parser("GET", url, Collections.emptyMap(), httpEntity, new Header[]{});
            Configuration configuration = Configuration.from(resultStr);
            if (configuration != null) {
                List<Map> results = configuration.getList("docs", Map.class);
                results.stream().forEach(map -> {
                    Boolean found = (Boolean) map.get("found");
                    if (found) {
                        successGetItems.add((Map) map.get("_source"));
                    } else {
                        notFoundIds.add(String.valueOf(map.get("_id")));
                    }
                });
                if (!notFoundIds.isEmpty()) {
                    LOGGER.error("部分ID值对应的文档未找到：notFoundIds = " + notFoundIds.toString());
                }
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("\nmget done. ResponseData:\n" + resultStr);
            }
        } catch (Exception e) {
            LOGGER.error("查找失败！", e);
            throw new ElasticsearchException("查找失败！", e);
        }
        return successGetItems;
    }

    /**
     * 批量保存 index指令：文档存在则更新、否则创建新的文档
     *
     * @param recordDataList 文档记录集
     * @param indexName      索引名
     * @return
     */
    public void batchSave(List<Map> recordDataList, String indexName) {
        Assert.notEmpty(recordDataList, "recordDataList不能为空！");
        Assert.hasText(indexName, "indexName 不能为空！");
        try {
            String url = "/" + indexName + "/doc/_bulk";
            final StringBuilder builder = new StringBuilder();
            recordDataList.stream().forEach(map -> {
                if (!map.containsKey("_id")) {
                    throw new IllegalArgumentException("要保存的记录的主键值\"_id\"缺失！");
                }
                builder.append("{\"index\":{\"_id\":\"").append(map.get("_id")).append("\"").append("}}").append("\n");
                map.remove("_id");
                builder.append(JSON.toJSONString(map, SerializerFeature.WriteMapNullValue)).append("\n");
            });

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(builder.toString());
            }
            ContentType contentType = ContentType.TEXT_PLAIN.withCharset(Charset.forName("UTF-8"));
            HttpEntity httpEntity = new StringEntity(builder.toString(), contentType);

            String resultStr = this.parser("POST", url, Collections.emptyMap(), httpEntity, new Header[]{new BasicHeader("Content-Type", "application/x-ndjson")});
            Configuration configuration = Configuration.from(resultStr);
            if (configuration != null) {
                Boolean hasErrors = configuration.getBool("errors");
                if (hasErrors) {//部分文档保存失败
                    List<Map> items = configuration.getList("items", Map.class);
                    List<Map> errorItems = items.stream().filter(map -> {
                        //                        Map tmp = (Map) map.get("update");
                        Map tmp = (Map) map.get("index");
                        if ((Integer) tmp.get("status") != HttpStatus.SC_OK
                                && (Integer) tmp.get("status") != HttpStatus.SC_CREATED) {
                            return true;
                        }
                        return false;
                    }).collect(Collectors.toList());
                    int total = items.size();
                    int failure = errorItems.size();
                    int success = total - failure;
                    LOGGER.error(String.format("批量保存操作完成，总记录数：{%s}，成功：{%s}，失败：{%s}，response：{%s}", total, success, failure, errorItems.toString()));
                    throw new ElasticsearchException("批量保存操作失败！");
                }
                refresh(indexName);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("\npost done. ResponseData:\n" + resultStr);
            }
        } catch (Exception e) {
            LOGGER.error("batchSave方法执行异常！", e);
            throw new ElasticsearchException("批量保存方法执行失败！", e);
        }
    }


    /**
     * 批量更新 update指令：文档存在则更新、否则报异常，单个文档失败不影响整个批量操作
     *
     * @param recordDataList 文档记录集
     * @param indexName      索引名
     * @return
     */
    public void batchUpdate(List<Map> recordDataList, String indexName) {
        Assert.notEmpty(recordDataList, "recordDataList不能为空！");
        Assert.hasText(indexName, "indexName 不能为空！");
        try {
            String url = "/" + indexName + "/doc/_bulk";
            final StringBuilder builder = new StringBuilder();
            recordDataList.stream().forEach(map -> {
                if (!map.containsKey("_id")) {
                    throw new IllegalArgumentException("要更新的记录的主键值\"_id\"缺失！");
                }
                builder.append("{\"update\":{\"_id\":\"").append(map.get("_id")).append("\"").append("}}").append("\n");
                map.remove("_id");
                String doc = "{\"doc\":" + JSON.toJSONString(map, SerializerFeature.WriteMapNullValue) + "}\n";
                builder.append(doc);
            });

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(builder.toString());
            }
            ContentType contentType = ContentType.TEXT_PLAIN.withCharset(Charset.forName("UTF-8"));
            HttpEntity httpEntity = new StringEntity(builder.toString(), contentType);
            String resultStr = this.parser("POST", url, Collections.emptyMap(), httpEntity, new Header[]{new BasicHeader("Content-Type", "application/x-ndjson")});
            Configuration configuration = Configuration.from(resultStr);
            if (configuration != null) {
                Boolean hasErrors = configuration.getBool("errors");
                if (hasErrors) {//部分文档保存失败
                    List<Map> items = configuration.getList("items", Map.class);
                    List<Map> errorItems = items.stream().filter(map -> {
                        //                        Map tmp = (Map) map.get("update");
                        Map tmp = (Map) map.get("index");
                        if ((Integer) tmp.get("status") != HttpStatus.SC_OK
                                && (Integer) tmp.get("status") != HttpStatus.SC_CREATED) {
                            return true;
                        }
                        return false;
                    }).collect(Collectors.toList());
                    int total = items.size();
                    int failure = errorItems.size();
                    int success = total - failure;
                    LOGGER.error(String.format("批量更新操作完成，总记录数：{%s}，成功：{%s}，失败：{%s}，response：{%s}", total, success, failure, errorItems.toString()));
                    throw new ElasticsearchException("批量更新操作失败！");
                }
                refresh(indexName);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("\npost done. ResponseData:\n" + resultStr);
            }
        } catch (Exception e) {
            LOGGER.error("batchUpdate方法执行异常！", e);
            throw new ElasticsearchException("批量更新方法执行失败！", e);
        }
    }

    /**
     * 批量更新 delete指令：文档存在则删除、否则报异常，单个文档失败不影响整个批量操作
     *
     * @param idLists   文档主键ID值记录集
     * @param indexName 索引名
     * @return
     */
    public void batchDelete(List<String> idLists, String indexName) {
        Assert.notEmpty(idLists, "idLists 不能为空！");
        Assert.hasText(indexName, "indexName 不能为空！");
        try {
            String url = "/" + indexName + "/doc/_bulk";
            final StringBuilder builder = new StringBuilder();
            idLists.stream().forEach(id -> {
                builder.append("{\"delete\":{\"_id\":\"").append(id).append("\"}}").append("\n");
            });

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(builder.toString());
            }
            ContentType contentType = ContentType.TEXT_PLAIN.withCharset(Charset.forName("UTF-8"));
            HttpEntity httpEntity = new StringEntity(builder.toString(), contentType);
            String resultStr = this.parser("POST", url, Collections.emptyMap(), httpEntity, new Header[]{new BasicHeader("Content-Type", "application/json;charset=UTF-8")});
            Configuration configuration = Configuration.from(resultStr);
            if (configuration != null) {
                List<Map> items = configuration.getList("items", Map.class);
                List<Map> errorItems = items.stream().filter(map -> {
                    Map tmp = (Map) map.get("delete");
                    if ((Integer) tmp.get("status") != HttpStatus.SC_OK) {
                        return true;
                    }
                    return false;
                }).collect(Collectors.toList());
                int total = items.size();
                int failure = errorItems.size();
                int success = total - failure;
                if (failure > 0) {
                    LOGGER.error(String.format("批量删除操作完成，总记录数：{%s}，成功：{%s}，失败：{%s}，response：{%s}", total, success, failure, errorItems.toString()));
                } else if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String.format("批量删除操作完成，总记录数：{%s}，成功：{%s}，失败：{%s}，response：{%s}", total, success, failure, errorItems.toString()));
                }
                refresh(indexName);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("\npost done. ResponseData:\n" + resultStr);
            }
        } catch (Exception e) {
            LOGGER.error("batchDelete方法执行异常！", e);
            throw new ElasticsearchException("批量删除方法执行失败！", e);
        }
    }

    /**
     * 刷新索引，让新增的记录及时可查
     *
     * @param indexName
     * @return
     */
    public void refresh(String indexName) {
        Assert.hasText(indexName, "indexName 不能为空！");
        try {
            String url = "/" + indexName + "/_refresh";
            Response response = restClient.performRequest("POST", url);
            if (response.getStatusLine() != null) {
                int httpStatus = response.getStatusLine().getStatusCode();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("\nrefresh operation done. HttpStatus:" + httpStatus);
                }
            }
        } catch (Exception e) {
            LOGGER.error("refresh方法执行异常！", e);
        }
    }

    /**
     * 读取大结果集的scroll方法
     *
     * @param dslStatement       DSL查询语句
     * @param timeWindow         时间窗口，单位分钟，默认值是1分钟
     * @param timeWindowCallBack 时间窗口回调接口
     * @param indexName          索引名
     * @param includeFields      需要返还的字段，多个用逗号分隔，字段名支持通配符
     * @param excludeFields      不需要返还的字段，多个用逗号分隔，字段名支持通配符
     *                           includeFields和excludeFields都为空时默认返还所有字段
     * @throws IOException
     */
    public void scroll(String dslStatement, String timeWindow, TimeWindowCallBack timeWindowCallBack, String indexName, String includeFields, String excludeFields) throws IOException {
       this.scrollSource(dslStatement, timeWindow, new TimeWindowCallBack() {
           @Override
           public void process(List<Map> tmpDataList) {
               List<Map> dataList = new ArrayList<>(tmpDataList.size());
               tmpDataList.forEach(map -> {
                   Map record = (Map) map.get("_source");
                   String id = (String) map.get("_id");
                   record.put("_id", id);
                   dataList.add(record);
               });
               //回调接口处理时间窗口返回的数据
               timeWindowCallBack.process(dataList);
           }
       },indexName,includeFields,excludeFields);
    }
    /**
     * 读取大结果集的scroll方法
     *
     * @param dslStatement       DSL查询语句
     * @param timeWindow         时间窗口，单位分钟，默认值是1分钟
     * @param timeWindowCallBack 时间窗口回调接口
     * @param indexName          索引名
     * @param includeFields      需要返还的字段，多个用逗号分隔，字段名支持通配符
     * @param excludeFields      不需要返还的字段，多个用逗号分隔，字段名支持通配符
     *                           includeFields和excludeFields都为空时默认返还所有字段
     * @throws IOException
     */
    public void scrollSource(String dslStatement, String timeWindow, TimeWindowCallBack timeWindowCallBack, String indexName, String includeFields, String excludeFields) throws IOException {
        Assert.hasText(dslStatement, "dslStatement 不能为空！");
        Assert.notNull(timeWindowCallBack, "timeWindowCallBack 不能为空！");
        // Assert.hasText(indexName, "indexName 不能为空！");

        HttpEntity httpEntity = new StringEntity(dslStatement, ContentType.APPLICATION_JSON);

        //第一步指定索引或者类型：/_search?scroll=1m
//        String scrolPreUrl = "/" + indexName + "/doc/_search";
        StringBuffer scrolPreUrl = new StringBuffer();
        if (StringUtils.isNotBlank(indexName)) {
            scrolPreUrl.append("/").append(indexName).append("/doc");
        }
        scrolPreUrl.append("/_search");
        if (Strings.isNullOrEmpty(timeWindow)) {
            scrolPreUrl.append("?scroll=1m");
        } else {
            scrolPreUrl.append("?scroll=").append(timeWindow).append("m");
        }

        if (!Strings.isNullOrEmpty(includeFields)) {
            scrolPreUrl.append("&_source_include=").append(includeFields);
        }
        if (!Strings.isNullOrEmpty(excludeFields)) {
            scrolPreUrl.append("&_source_exclude=").append(excludeFields);
        }

        int searchCount = 1;//查询请求计数器
        Response response = restClient.performRequest("GET", scrolPreUrl.toString(), Collections.<String, String>emptyMap(), httpEntity);

        int total = 0;
        int shardTotal = 0;
        boolean hasShardFailed = false;
        int count = 0;
        String scrollId = null;
        String resultStr = this.parser(response);
        if (resultStr != null) {
            Configuration configuration = Configuration.from(resultStr);
            total = configuration.getInt("hits.total");
            shardTotal = configuration.getInt("_shards.total");
            int shardFailed = configuration.getInt("_shards.failed");
            if (shardFailed > 0) {
                LOGGER.error(String.format("searchCount[%s]-查询index[%s]有分片读取失败", searchCount, indexName));
                hasShardFailed = true;
            }
            scrollId = configuration.getString("_scroll_id");
            List<Map> tmpDataList = configuration.getList("hits.hits", Map.class);
            timeWindowCallBack.process(tmpDataList);
            count += tmpDataList.size();
        }
        //第二步URL不能包含index和type名称-在第一步指定
        String scrollUrl = "/_search/scroll?scroll_id=" + scrollId;
        if (Strings.isNullOrEmpty(timeWindow)) {
            scrollUrl += "&scroll=1m";
        } else {
            scrollUrl += "&scroll=" + timeWindow + "m";
        }

        while (true) {
            searchCount++;
            response = restClient.performRequest("GET", scrollUrl, Collections.<String, String>emptyMap());

            resultStr = this.parser(response);
            if (resultStr != null) {
                Configuration configuration = Configuration.from(resultStr);
                scrollId = configuration.getString("_scroll_id");
                int shardFailed = configuration.getInt("_shards.failed");
                if (shardFailed > 0) {
                    LOGGER.error(String.format("searchCount[%s]-查询index[%s], shardTotal[%s], 有[%s]个分片读取失败！", searchCount, indexName, shardTotal, shardFailed));
                    hasShardFailed = true;
                }
                List<Map> tmpDataList = configuration.getList("hits.hits", Map.class);
                if (tmpDataList.size() == 0) {//读取完毕，跳出循环
                    break;
                }
                //回调接口处理时间窗口返回的数据
                timeWindowCallBack.process(tmpDataList);
                count += tmpDataList.size();
            }
        }
        LOGGER.info(String.format("index: %s, 来源总记录数：%s条, 读取的总记录数：%s条, 发起网络请求数：%s次, 是否有分片读取失败：%s", indexName, total, count, searchCount, hasShardFailed));
        //释放search context查询上下文的资源
        response = restClient.performRequest("DELETE", "/_search/scroll?scroll_id=" + scrollId);
    }

    /**
     * 查询数据
     *
     * @param dsl       查询的dsl语句
     * @param indexName 查询es的index名称
     * @throws IOException
     */
    private String queryByDsl(String dsl, String indexName) throws IOException {
        String endpoint = "/_search";
        if (StringUtils.isNotBlank(indexName)) {
            endpoint = "/" + indexName + endpoint;
        }
        Map<String, String> params = Collections.<String, String>emptyMap();
        Header[] headers = new Header[]{};
        HttpEntity entity = new NStringEntity(dsl, ContentType.APPLICATION_JSON);
        return this.parser("GET", endpoint, params, entity, headers);
    }

    /**
     * 查询数据
     *
     * @param dsl       查询的dsl语句
     * @param indexName 查询到es的index名称
     * @throws IOException
     */
    public Map query(String dsl, String indexName) throws IOException {
        String resultStr = this.queryByDsl(dsl, indexName);
        Map resultMap = new HashMap();
        try {
            Configuration result = Configuration.from(resultStr);
            Configuration all = result.getConfiguration("hits");
            int total = all.getInt("total");
            List<Object> hits = this.parseHits(all);
            LOGGER.debug(JSON.toJSONString(hits));
            resultMap.put("total", total);
            resultMap.put("data", hits);
            resultMap.put("aggs", result.get("aggregations"));

        } catch (RuntimeException e) {
            throw new IOException("返回结果解析异常", e);
        }
        return resultMap;
    }

    /**
     * 聚合数据
     *
     * @param dsl       聚合查询的dsl语句
     * @param indexName 查询es的index名称
     * @throws IOException
     */
    public Map aggs(String dsl, String indexName) throws IOException {
        String resultStr = this.queryByDsl(dsl, indexName);

        Map resultMap = new HashMap();
        try {
            Configuration result = Configuration.from(resultStr);
            Configuration all = result.getConfiguration("hits");
            int total = all.getInt("total");
            List<Object> hits = this.parseHits(all);
            Map<String, Object> aggs = this.parseAggs(result);
            LOGGER.debug(JSON.toJSONString(hits));
            resultMap.put("total", total);
            resultMap.put("data", hits);
            resultMap.put("aggs", aggs);

        } catch (RuntimeException e) {
            throw new IOException("返回结果解析异常", e);
        }
        return resultMap;
    }

    private List<Object> parseHits(Configuration all) {
        List<Object> hits = all.getList("hits");
        hits.forEach(h -> {
            Map hit = (Map) h;
            Map src = (Map) hit.remove("_source");
            hit.putAll(src);
        });
        return hits;
    }

    private Map parseAggs(Configuration result) {
        Map aggs = result.getMap("aggregations");
        Map group = new HashMap();
        aggs.forEach((groupName, ag) -> {
            Map g = (Map) ag;
            Object value = g.get("buckets");
            if (value == null) {
                value = g.get("value");
            }
            group.put(groupName, value);
        });
        return group;
    }

    /**
     * 时间窗口回调接口，处理每一次时间窗口返回的数据
     */
    public interface TimeWindowCallBack {
        void process(List<Map> var1);
    }
}

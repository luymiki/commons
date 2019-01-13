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

package com.anluy.commons.service;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.anluy.commons.BaseEntity;
import com.anluy.commons.dao.BaseDAO;
import com.anluy.commons.elasticsearch.ElasticsearchRestClient;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 功能说明：
 * <p>
 * Created by hc.zeng on 2017/8/6.
 */
public abstract class BaseServiceImpl<PK, T extends BaseEntity<PK>> implements BaseService<PK, T> {

    /**
     * 数据库操作对象
     *
     * @return
     */
    public abstract BaseDAO getBaseDAO();

    /**
     * es查询对象
     *
     * @return
     */
    public abstract ElasticsearchRestClient getElasticsearchRestClient();

    public abstract String getIndexName();

    /**
     * 保存一条数据
     *
     * @param entity
     */
    public T save(T entity) {
        if (StringUtils.isBlank((String)entity.getId())) {
            String uuid = UUID.randomUUID().toString();
            entity.setId((PK) uuid);
        }
        //如果有es操作对象，操作es
        if (this.getElasticsearchRestClient() != null && StringUtils.isNotBlank(this.getIndexName())) {
            Map<String, Object> jsonMap = (Map<String, Object>) JSON.toJSON(entity);
            jsonMap.forEach((field,value)->{
                jsonMap.put(field,formater(field,value));
            });
            this.getElasticsearchRestClient().save(jsonMap, entity.getId().toString(), this.getIndexName());
        }
        try{
            getBaseDAO().save(entity);
        }catch (RuntimeException e){
            if (this.getElasticsearchRestClient() != null && StringUtils.isNotBlank(this.getIndexName())) {
                this.getElasticsearchRestClient().remove(entity.getId().toString(), this.getIndexName());
            }
            throw e;
        }
        return entity;
    }


    /**
     * 根据主键查询
     *
     * @param id
     * @return
     */
    public T get(PK id) {
        return (T) getBaseDAO().get(id);
    }

    /**
     * 查询所有的数据
     *
     * @return
     */
    public List<T> getAll() {
        return getBaseDAO().getAll();
    }

    /**
     * 更新一条数据
     *
     * @param entity
     */
    public int update(T entity) {
        Map map = null;
        //如果有es操作对象，操作es
        if (this.getElasticsearchRestClient() != null && StringUtils.isNotBlank(this.getIndexName())) {
            Map<String, Object> jsonMap = (Map<String, Object>) JSON.toJSON(entity);
            jsonMap.forEach((field,value)->{
                jsonMap.put(field,formater(field,value));
            });
            //查询原始数据，
            map = this.getElasticsearchRestClient().get(this.getIndexName(),entity.getId().toString(),null,null);
            //修改
            this.getElasticsearchRestClient().update(jsonMap, entity.getId().toString(), this.getIndexName());
        }
        try{
            return getBaseDAO().update(entity);
        }catch (RuntimeException e){
            if(this.getElasticsearchRestClient() != null && StringUtils.isNotBlank(this.getIndexName())&& map!=null){
                this.getElasticsearchRestClient().update(map, entity.getId().toString(), this.getIndexName());
            }
            throw e;
        }
    }

    /**
     * 删除一条记录
     *
     * @param id
     * @return
     */
    public int delete(PK id) {
        Map map = null;
        //如果有es操作对象，操作es
        if (this.getElasticsearchRestClient() != null && StringUtils.isNotBlank(this.getIndexName())) {
            //查询原始数据，
            map = this.getElasticsearchRestClient().get(this.getIndexName(),id.toString(),null,null);
            //删除
            this.getElasticsearchRestClient().remove(id.toString(), this.getIndexName());
        }
        try{
            return getBaseDAO().delete(id);
        }catch (RuntimeException e){
            if(this.getElasticsearchRestClient() != null && StringUtils.isNotBlank(this.getIndexName())&& map!=null){
                this.getElasticsearchRestClient().save(map, id.toString(), this.getIndexName());
            }
            throw e;
        }

    }

}

package com.anluy.commons.service;

import com.alibaba.fastjson.JSON;
import com.anluy.commons.BaseEntity;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 功能说明：
 * <p>
 * Created by hc.zeng on 2018/3/19.
 */
public interface BaseService<PK, T extends BaseEntity<PK>> {
    /**
     * 字段格式化方法
     * @param field
     * @param value
     * @return
     */
    default Object formater(String field,Object value){
        return value;
    }

    /**
     * 保存一条数据
     *
     * @param entity
     */
    T save(T entity);


    /**
     * 根据主键查询
     *
     * @param id
     * @return
     */
    T get(PK id);

    /**
     * 查询所有的数据
     *
     * @return
     */
    List<T> getAll();

    /**
     * 更新一条数据
     *
     * @param entity
     */
    int update(T entity);

    /**
     * 删除一条记录
     *
     * @param id
     * @return
     */
    int delete(PK id);

}

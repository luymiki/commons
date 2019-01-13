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

package com.anluy.commons.dao;


import com.anluy.commons.BaseEntity;

import java.util.List;

/**
 * 功能说明：
 * <p>
 * Created by hc.zeng on 2017/8/6.
 */
public interface BaseDAO<PK,T extends BaseEntity<PK>> {
    /**
     * 根据主键查询
     * @param id
     * @return
     */
    T get(PK id);

    /**
     * 查询所有的数据
     * @return
     */
    List<T> getAll();

    /**
     * 保存一条数据
     * @param entity
     */
    void save(T entity);

    /**
     * 更新一条数据
     * @param entity
     */
    int update(T entity);

    /**
     * 删除一条记录
     * @param id
     * @return
     */
    int delete(PK id);

}

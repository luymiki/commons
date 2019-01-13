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

package com.anluy.commons.utils;

import java.util.*;
import java.util.regex.Pattern;

/**
 * 提供从 List<String> 中根据 regular 过滤的通用工具(返回值已经去重). 使用场景，比如：odpsreader
 * 的分区筛选，hdfsreader/txtfilereader的路径筛选等
 */
public final class FilterUtil {

    //已经去重
    public static List<String> filterByRegular(List<String> allStrs,
                                               String regular) {
        List<String> matchedValues = new ArrayList<String>();

        // 语法习惯上的兼容处理(pt=* 实际正则应该是：pt=.*)
        String newReqular = regular.replace(".*", "*").replace("*", ".*");

        Pattern p = Pattern.compile(newReqular);

        for (String partition : allStrs) {
            if (p.matcher(partition).matches()) {
                if (!matchedValues.contains(partition)) {
                    matchedValues.add(partition);
                }
            }
        }

        return matchedValues;
    }

    //已经去重
    public static List<String> filterByRegulars(List<String> allStrs,
                                                List<String> regulars) {
        List<String> matchedValues = new ArrayList<String>();

        List<String> tempMatched = null;
        for (String regular : regulars) {
            tempMatched = filterByRegular(allStrs, regular);
            if (null != tempMatched && !tempMatched.isEmpty()) {
                for (String temp : tempMatched) {
                    if (!matchedValues.contains(temp)) {
                        matchedValues.add(temp);
                    }
                }
            }
        }

        return matchedValues;
    }
}

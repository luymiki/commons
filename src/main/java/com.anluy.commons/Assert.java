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

package com.anluy.commons;

import org.apache.commons.lang3.StringUtils;

/**
 * ${DESCRIPTION}
 *
 * @author hc.zeng
 * @create 2017-11-04 15:30
 */

public class Assert {
    private Assert(){}

    /**
     * 断言字符串为空，如果为空，抛出异常
     * @param cs
     * @param errorCode
     * @param message
     * @throws AssertException
     */
    public static void isBlank(CharSequence cs,int errorCode,String message) throws AssertException {
        if(StringUtils.isBlank(cs)){
            throw  new AssertException(errorCode,message);
        }
    }


}

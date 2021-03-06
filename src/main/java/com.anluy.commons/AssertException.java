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

/**
 * 断言工具异常
 *
 * @author hc.zeng
 * @create 2017-11-04 15:32
 */

public class AssertException extends Exception{
    private int errorCode = 100000;
    private String message = "系统异常";

    public AssertException(int errorCode,String message) {
        super(message);
        this.errorCode = errorCode;
        this.message = message;
    }

    public int getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}

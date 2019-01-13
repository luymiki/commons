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

package com.anluy.commons.web;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 功能说明：
 * <p>
 * Created by hc.zeng on 2017/8/6.
 */
public class Result {

    private String timestamp;
    private int status = 200;
    private String error;
    private String exception;
    private String message;
    private Object data;
    private String path;

    public Result() {
        this.timestamp = formatTimestamp();
    }

    public Result(int status, String message) {
        this.status = status;
        this.message = message;
        timestamp = formatTimestamp();
    }

    public String getTimestamp() {
        return timestamp;
    }

    public Result setTimestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getException() {
        return exception;
    }

    public Result setException(String exception) {
        this.exception = exception;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public Result setData(Object data) {
        this.data = data;
        return this;
    }

    public String getPath() {
        return path;
    }

    public Result setPath(String path) {
        this.path = path;
        return this;
    }

    public static Result seuccess(String message){
        return new Result(200,message);
    }

    public static Result seuccess(String message,Object data){
        return new Result(200,message).setData(data);
    }

    public static Result error(int code,String message){
        return new Result(code,message);
    }

    public static Result error(int code,String message,String exception){
        return new Result(code,message).setException(exception);
    }

    private String formatTimestamp(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return  sdf.format(new Date());
    }

}

///*
// * Copyright 2017 com.anluy
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.anluy.commons.web;
//
//import com.alibaba.fastjson.JSON;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.autoconfigure.web.*;
//import org.springframework.boot.context.properties.EnableConfigurationProperties;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.MediaType;
//import org.springframework.http.ResponseEntity;
//import org.springframework.stereotype.Controller;
//import org.springframework.validation.BindingResult;
//import org.springframework.validation.FieldError;
//import org.springframework.web.bind.MethodArgumentNotValidException;
//import org.springframework.web.bind.MissingServletRequestParameterException;
//import org.springframework.web.bind.annotation.RequestMapping;
//import org.springframework.web.bind.annotation.ResponseBody;
//
//import javax.servlet.http.HttpServletRequest;
//import java.util.Date;
//import java.util.List;
//import java.util.Map;
//
///**
// * 功能说明：通用错误处理器
// * <p>
// * Created by hc.zeng on 2017/9/3.
// */
//@Controller
//@RequestMapping(value = "error")
//@EnableConfigurationProperties({ServerProperties.class})
//public class ControllerExceptionHandler extends AbstractErrorController {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerExceptionHandler.class);
//    private static final String ERROR_ATTRIBUTE = DefaultErrorAttributes.class.getName() + ".ERROR";
//
//    @Autowired
//    private ServerProperties serverProperties;
//
//    public ControllerExceptionHandler(ErrorAttributes errorAttributes) {
//        super(errorAttributes);
//    }
//
//    public Exception getException(HttpServletRequest request) {
//        return (Exception) request.getAttribute(ERROR_ATTRIBUTE);
//    }
//
//    private Result getResult(HttpServletRequest request) {
//        Map<String, Object> error = getErrorAttributes(request, isIncludeStackTrace(request, MediaType.TEXT_HTML));
//        if (error.containsKey("timestamp")) {
//            Date d = (Date) error.get("timestamp");
//            error.put("timestamp", d.getTime());
//        }
//        return JSON.toJavaObject((JSON) JSON.toJSON(error), Result.class);
//    }
//
//
//    /**
//     * 定义404错误处理
//     *
//     * @param request
//     * @return
//     */
//    @RequestMapping(value = "400")
//    @ResponseBody
//    public ResponseEntity<Result> error400(HttpServletRequest request) {
//        Exception ex = getException(request);
//        if (ex != null && ex instanceof MissingServletRequestParameterException) {
//            MissingServletRequestParameterException mex = (MissingServletRequestParameterException) ex;
//            StringBuffer msg = new StringBuffer();
//            msg.append("Required String parameter '").append(mex.getParameterName()).append("'[").append(mex.getParameterType()).append("]").append(" is not present");
//            Result result = getResult(request);
//            result.setMessage("参数不合法:" + msg.toString());
//            return new ResponseEntity<Result>(result, HttpStatus.OK);
//        }else if (ex != null && ex instanceof MethodArgumentNotValidException) {
//            MethodArgumentNotValidException mex = (MethodArgumentNotValidException) ex;
//            BindingResult bresult = mex.getBindingResult();
//            List<FieldError> fieldErrors = bresult.getFieldErrors();
//            StringBuffer msg = new StringBuffer();
//            fieldErrors.stream().forEach(fieldError -> {
//                msg.append("[" + fieldError.getField() + "," + fieldError.getDefaultMessage() + "]");
//            });
//            Result result = getResult(request);
//            result.setMessage("参数不合法:" + msg.toString());
//            return new ResponseEntity<Result>(result, HttpStatus.OK);
//        }
//        Result result = getResult(request);
//        result.setMessage("参数不合法");
//        return new ResponseEntity<Result>(result, HttpStatus.OK);
//    }
//
//    /**
//     * 定义404错误处理
//     *
//     * @param request
//     * @return
//     */
//    @RequestMapping(value = "404")
//    @ResponseBody
//    public ResponseEntity<Result> error404(HttpServletRequest request) {
//        Result result = getResult(request);
//        result.setMessage("访问的资源不存在");
//        LOGGER.error(String.format("!!! request uri:%s from %s not found exception:%s", request.getRequestURI(), request.getLocalAddr(), result.getPath()));
//        return new ResponseEntity<Result>(result, HttpStatus.OK);
//    }
//
//    /**
//     * 定义500错误处理
//     *
//     * @param request
//     * @return
//     */
//    @RequestMapping(value = "500")
//    @ResponseBody
//    public ResponseEntity<Result> error500(HttpServletRequest request, Exception ex) {
//        Result result = getResult(request);
//        result.setMessage("系统繁忙,请稍候重试");
//        LOGGER.error("!!! request uri:{} from {} server exception:{}", request.getRequestURI(), request.getLocalAddr(), ex.getMessage());
//        return new ResponseEntity<Result>(result, HttpStatus.OK);
//    }
//
//    protected boolean isIncludeStackTrace(HttpServletRequest request, MediaType produces) {
//        ErrorProperties.IncludeStacktrace include = this.serverProperties.getError().getIncludeStacktrace();
//        return include == ErrorProperties.IncludeStacktrace.ALWAYS ? true : (include == ErrorProperties.IncludeStacktrace.ON_TRACE_PARAM ? this.getTraceParameter(request) : false);
//    }
//
//    @Override
//    public String getErrorPath() {
//        return null;
//    }
//}

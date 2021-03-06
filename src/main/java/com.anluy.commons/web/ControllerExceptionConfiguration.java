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
//import org.springframework.boot.SpringBootConfiguration;
//import org.springframework.boot.context.embedded.ConfigurableEmbeddedServletContainer;
//import org.springframework.boot.context.embedded.EmbeddedServletContainerCustomizer;
//import org.springframework.boot.web.servlet.ErrorPage;
//import org.springframework.context.annotation.Bean;
//import org.springframework.http.HttpStatus;
////
/////**
//// * 功能说明：公共异常拦截配置
//// * <p>
//// * Created by hc.zeng on 2017/9/4.
//// */
////@SpringBootConfiguration
////public class ControllerExceptionConfiguration {
////
////    /**
////     * 配置默认错误处理的跳转路径
////     * @return
////     */
////    @Bean
////    public EmbeddedServletContainerCustomizer containerCustomizer(){
////        return new EmbeddedServletContainerCustomizer(){
////            @Override
////            public void customize(ConfigurableEmbeddedServletContainer container) {
////                container.addErrorPages(new ErrorPage(HttpStatus.NOT_FOUND, "/error/404"));
////                container.addErrorPages(new ErrorPage(HttpStatus.BAD_REQUEST, "/error/400"));
////                container.addErrorPages(new ErrorPage(HttpStatus.INTERNAL_SERVER_ERROR, "/error/500"));
////                container.addErrorPages(new ErrorPage(Throwable.class,"/error/500"));
////            }
////        };
////    }
////
////}

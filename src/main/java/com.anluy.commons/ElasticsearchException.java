package com.anluy.commons;

/**
 * 功能说明：
 * <p>
 * Created by hc.zeng on 2018/3/19.
 */
public class ElasticsearchException extends RuntimeException {
    public ElasticsearchException() {
    }

    public ElasticsearchException(String message) {
        super(message);
    }

    public ElasticsearchException(String message, Throwable cause) {
        super(message, cause);
    }

    public ElasticsearchException(Throwable cause) {
        super(cause);
    }

    public ElasticsearchException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

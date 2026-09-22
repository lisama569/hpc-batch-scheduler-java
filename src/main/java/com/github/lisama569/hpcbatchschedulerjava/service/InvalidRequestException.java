package com.github.lisama569.hpcbatchschedulerjava.service;

/** 非法请求（字段缺失、需求非法等），HTTP 侧映射为 400。 */
public class InvalidRequestException extends RuntimeException {
    public InvalidRequestException(String message) {
        super(message);
    }
}

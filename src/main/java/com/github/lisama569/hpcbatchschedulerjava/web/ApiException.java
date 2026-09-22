package com.github.lisama569.hpcbatchschedulerjava.web;

import com.github.lisama569.hpcbatchschedulerjava.scheduler.ReasonCode;

/** 携带原因码的 API 异常，由全局处理器转换为结构化错误响应。 */
public class ApiException extends RuntimeException {

    private final ReasonCode reasonCode;
    private final int httpStatus;

    public ApiException(ReasonCode reasonCode, String message, int httpStatus) {
        super(message);
        this.reasonCode = reasonCode;
        this.httpStatus = httpStatus;
    }

    public ReasonCode getReasonCode() {
        return reasonCode;
    }

    public int getHttpStatus() {
        return httpStatus;
    }
}

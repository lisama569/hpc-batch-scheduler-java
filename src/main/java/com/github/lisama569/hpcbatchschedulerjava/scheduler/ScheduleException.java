package com.github.lisama569.hpcbatchschedulerjava.scheduler;

/**
 * 调度核心抛出的业务异常，携带可区分的原因码。
 * 既可供进程内调用方捕获，也由 HTTP 层统一映射为结构化错误响应。
 */
public class ScheduleException extends RuntimeException {

    private final ReasonCode reasonCode;

    public ScheduleException(ReasonCode reasonCode, String message) {
        super(message);
        this.reasonCode = reasonCode;
    }

    public ReasonCode getReasonCode() {
        return reasonCode;
    }
}

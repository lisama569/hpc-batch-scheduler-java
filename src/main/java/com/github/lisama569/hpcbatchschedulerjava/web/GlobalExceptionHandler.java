package com.github.lisama569.hpcbatchschedulerjava.web;

import com.github.lisama569.hpcbatchschedulerjava.dto.ApiError;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.ReasonCode;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.ScheduleException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 把业务异常映射为 {reasonCode, message} 的结构化响应，
 * 让调用方可以稳定地区分“非法请求 / 重复 / 未找到 / 状态冲突”等失败原因。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<ApiError> handleApi(ApiException ex) {
        return ResponseEntity.status(ex.getHttpStatus())
                .body(new ApiError(ex.getReasonCode().name(), ex.getMessage()));
    }

    @ExceptionHandler(ScheduleException.class)
    public ResponseEntity<ApiError> handleSchedule(ScheduleException ex) {
        return ResponseEntity.status(httpStatus(ex.getReasonCode()))
                .body(new ApiError(ex.getReasonCode().name(), ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> handleIllegal(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(new ApiError(ReasonCode.INVALID_REQUEST.name(), ex.getMessage()));
    }

    private HttpStatus httpStatus(ReasonCode code) {
        return switch (code) {
            case INVALID_REQUEST, IDEMPOTENCY_KEY_CONFLICT, DEPENDENCY_CYCLE -> HttpStatus.BAD_REQUEST;
            case JOB_NOT_FOUND, NODE_NOT_FOUND, UNKNOWN_USER, DEPENDENCY_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case JOB_STATE_CONFLICT, DUPLICATE_SUBMISSION, DEPENDENCY_FAILED -> HttpStatus.CONFLICT;
            case QUOTA_JOB_LIMIT, QUOTA_CPU_LIMIT, QUOTA_MEMORY_LIMIT,
                 INSUFFICIENT_NODES, NO_FITTING_NODE, INSUFFICIENT_CAPACITY,
                 PREEMPTION_INSUFFICIENT, WAITING_ON_DEPENDENCIES -> HttpStatus.UNPROCESSABLE_ENTITY;
            case NONE -> HttpStatus.OK;
        };
    }
}

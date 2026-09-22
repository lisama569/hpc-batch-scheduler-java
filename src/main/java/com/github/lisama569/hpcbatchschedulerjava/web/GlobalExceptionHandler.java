package com.github.lisama569.hpcbatchschedulerjava.web;

import com.github.lisama569.hpcbatchschedulerjava.service.DuplicateSubmissionException;
import com.github.lisama569.hpcbatchschedulerjava.service.InvalidRequestException;
import com.github.lisama569.hpcbatchschedulerjava.web.dto.ErrorResponse;
import com.github.lisama569.hpcbatchschedulerjava.web.dto.JobResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 全局异常映射：
 * <ul>
 *   <li>{@link InvalidRequestException}（含非法字段、不存在目标）→ 400 INVALID_REQUEST；</li>
 *   <li>{@link DuplicateSubmissionException}（幂等键重复）→ 409 DUPLICATE_SUBMISSION，
 *       并携带既有作业快照，调用方可据此区分“拒绝原因”。</li>
 * </ul>
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(InvalidRequestException.class)
    public ResponseEntity<ErrorResponse> handleInvalid(InvalidRequestException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ErrorResponse("INVALID_REQUEST", ex.getMessage(), null, java.util.List.of()));
    }

    @ExceptionHandler(DuplicateSubmissionException.class)
    public ResponseEntity<Map<String, Object>> handleDuplicate(DuplicateSubmissionException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of(
                        "code", "DUPLICATE_SUBMISSION",
                        "message", ex.getMessage(),
                        "existingJob", JobResponse.from(ex.existingJob())
                ));
    }
}

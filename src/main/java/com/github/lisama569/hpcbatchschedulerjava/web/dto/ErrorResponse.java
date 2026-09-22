package com.github.lisama569.hpcbatchschedulerjava.web.dto;

/** 统一错误响应：携带可区分的原因码与判定依据。 */
public record ErrorResponse(String code, String message, String existingJobId, java.util.List<String> details) {
}

package com.github.lisama569.hpcbatchschedulerjava.dto;

/** 提交作业响应：携带状态、原因码、作业状态与判定说明。 */
public record SubmitJobResponse(String jobId,
                                String status,
                                String reasonCode,
                                String message,
                                String jobState) {
}

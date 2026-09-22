package com.github.lisama569.hpcbatchschedulerjava.dto;

/** 提交作业请求。 */
public record SubmitJobRequest(String idempotencyKey,
                               String user,
                               Integer priority,
                               Integer nodeCount,
                               Long cpu,
                               Long memoryMb,
                               Long gpu) {
}

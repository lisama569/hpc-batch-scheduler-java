package com.github.lisama569.hpcbatchschedulerjava.dto;

import java.util.List;

/** 提交作业请求。dependsOn 为前置作业引用（jobId 或幂等键），可空。 */
public record SubmitJobRequest(String idempotencyKey,
                               String user,
                               Integer priority,
                               Integer nodeCount,
                               Long cpu,
                               Long memoryMb,
                               Long gpu,
                               List<String> dependsOn) {
}

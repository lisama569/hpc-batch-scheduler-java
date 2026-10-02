package com.github.lisama569.hpcbatchschedulerjava.dto;

import java.util.List;

/** 提交作业请求。dependsOn 为可选的前置作业 id 列表。 */
public record SubmitJobRequest(String idempotencyKey,
                               String user,
                               Integer priority,
                               Integer nodeCount,
                               Long cpu,
                               Long memoryMb,
                               Long gpu,
                               List<String> dependsOn) {
}

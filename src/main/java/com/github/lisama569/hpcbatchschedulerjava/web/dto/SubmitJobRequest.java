package com.github.lisama569.hpcbatchschedulerjava.web.dto;

import java.util.List;

/** 提交作业请求。 */
public record SubmitJobRequest(
        String idempotencyKey,
        String owner,
        Integer priority,
        List<TaskRequest> tasks
) {
}

package com.github.lisama569.hpcbatchschedulerjava.web.dto;

import com.github.lisama569.hpcbatchschedulerjava.model.Job;

import java.util.List;
import java.util.Map;

/** 作业视图，包含最新一次判定依据与落点。 */
public record JobResponse(
        String id,
        String idempotencyKey,
        String owner,
        int priority,
        String state,
        String lastReason,
        Map<Integer, String> placement,
        List<String> decisionLog,
        int preemptCount,
        String lastPreemptedBy
) {
    public static JobResponse from(Job job) {
        return new JobResponse(
                job.id(),
                job.idempotencyKey(),
                job.owner(),
                job.priority(),
                job.state().name(),
                job.lastReason() == null ? null : job.lastReason().name(),
                job.placement(),
                job.decisionLog(),
                job.preemptCount(),
                job.lastPreemptedByJobId()
        );
    }
}

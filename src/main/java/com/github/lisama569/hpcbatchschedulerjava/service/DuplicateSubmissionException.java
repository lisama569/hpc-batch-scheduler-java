package com.github.lisama569.hpcbatchschedulerjava.service;

import com.github.lisama569.hpcbatchschedulerjava.model.Job;

/** 相同幂等标识的重复提交，HTTP 侧映射为 409，并携带既有作业结论。 */
public class DuplicateSubmissionException extends RuntimeException {
    private final Job existingJob;
    public DuplicateSubmissionException(Job existingJob) {
        super("duplicate idempotency key: " + existingJob.idempotencyKey());
        this.existingJob = existingJob;
    }
    public Job existingJob() {
        return existingJob;
    }
}

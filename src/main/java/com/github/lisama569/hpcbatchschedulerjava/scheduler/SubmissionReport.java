package com.github.lisama569.hpcbatchschedulerjava.scheduler;

import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;

/**
 * 提交的统一结论。HTTP 入口与进程内入口返回同一对象，保证结论一致、可解释。
 */
public record SubmissionReport(String jobId,
                               SubmissionStatus status,
                               ReasonCode reasonCode,
                               String message,
                               JobState jobState) {
}

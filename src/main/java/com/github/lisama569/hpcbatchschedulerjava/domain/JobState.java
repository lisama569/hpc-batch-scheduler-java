package com.github.lisama569.hpcbatchschedulerjava.domain;

/** 作业状态：排队 / 运行 / 已完成 / 已取消 / 被抢占。 */
public enum JobState {
    QUEUED,
    RUNNING,
    COMPLETED,
    CANCELLED,
    PREEMPTED
}

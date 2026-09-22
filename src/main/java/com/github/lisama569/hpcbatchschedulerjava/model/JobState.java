package com.github.lisama569.hpcbatchschedulerjava.model;

/** 作业生命周期状态。被抢占的作业会重新回到 QUEUED。 */
public enum JobState {
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED
}

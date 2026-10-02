package com.github.lisama569.hpcbatchschedulerjava.domain;

/** 作业状态：等待前置 / 排队 / 运行 / 已完成 / 已取消 / 被抢占 / 前置失败（终态）。 */
public enum JobState {
    /** 等待前置作业完成：不占资源、不计配额、不参与调度扫描。 */
    WAITING,
    QUEUED,
    RUNNING,
    COMPLETED,
    CANCELLED,
    PREEMPTED,
    /** 终态：前置作业未成功结束（被取消/被抢占/前置自身失败），本作业不会运行。 */
    DEPENDENCY_FAILED
}

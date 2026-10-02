package com.github.lisama569.hpcbatchschedulerjava.domain;

/** 作业状态：等待前置 / 排队 / 运行 / 已完成 / 已取消 / 被抢占 / 前置失败。 */
public enum JobState {
    /** 等待前置作业完成：不占资源、不计配额、不参与调度扫描。 */
    WAITING,
    QUEUED,
    RUNNING,
    COMPLETED,
    CANCELLED,
    PREEMPTED,
    /** 终态：前置作业未成功（被取消/被抢占/前置本身前置失败），依赖链向下传递。 */
    DEPENDENCY_FAILED
}

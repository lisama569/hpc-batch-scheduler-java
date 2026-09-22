package com.github.lisama569.hpcbatchschedulerjava.domain;

/** 节点状态：健康可调度 / 排空中（不再接收新作业）/ 不可用。 */
public enum NodeState {
    HEALTHY,
    DRAINING,
    UNAVAILABLE
}

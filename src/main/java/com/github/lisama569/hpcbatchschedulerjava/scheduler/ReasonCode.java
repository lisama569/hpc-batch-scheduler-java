package com.github.lisama569.hpcbatchschedulerjava.scheduler;

/**
 * 拒绝/排队原因码，保证非法请求、重复请求、容量不足等场景可被程序化区分。
 */
public enum ReasonCode {
    NONE,
    /** 非法请求：参数缺失或越界（节点数、资源量为负等）。 */
    INVALID_REQUEST,
    /** 非法请求：相同幂等键但提交内容不一致。 */
    IDEMPOTENCY_KEY_CONFLICT,
    /** 重复请求：相同幂等键的重复提交。 */
    DUPLICATE_SUBMISSION,
    /** 用户未配置配额。 */
    UNKNOWN_USER,
    /** 超出用户配额：运行作业数过多。 */
    QUOTA_JOB_LIMIT,
    /** 超出用户配额：CPU 总量超上限。 */
    QUOTA_CPU_LIMIT,
    /** 超出用户配额：内存总量超上限。 */
    QUOTA_MEMORY_LIMIT,
    /** 资源不足：可调度健康节点数量不足。 */
    INSUFFICIENT_NODES,
    /** 资源不足：没有任何节点能满足每节点资源（容量硬上限）。 */
    NO_FITTING_NODE,
    /** 资源不足：当前空闲资源不足（可在资源释放后再满足）。 */
    INSUFFICIENT_CAPACITY,
    /** 抢占后仍无法满足成组需求。 */
    PREEMPTION_INSUFFICIENT,
    /** 等待前置作业：声明的依赖尚未全部成功完成（不占用资源、不计配额）。 */
    WAITING_ON_DEPENDENCIES,
    /** 非法依赖：引用了不存在的作业。 */
    DEPENDENCY_NOT_FOUND,
    /** 非法依赖：直接或间接成环（含自引用）。 */
    DEPENDENCY_CYCLE,
    /** 终态原因：前置作业未成功结束（取消/抢占/前置失败级联）。 */
    DEPENDENCY_FAILED,
    /** 作业不存在。 */
    JOB_NOT_FOUND,
    /** 作业当前状态不允许该操作。 */
    JOB_STATE_CONFLICT,
    /** 节点不存在。 */
    NODE_NOT_FOUND
}

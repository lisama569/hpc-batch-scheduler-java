package com.github.lisama569.hpcbatchschedulerjava.scheduler;

/** 提交结果类别：接受（可能排队）/ 重复幂等请求 / 非法或被拒绝。 */
public enum SubmissionStatus {
    ACCEPTED,
    DUPLICATE,
    REJECTED
}

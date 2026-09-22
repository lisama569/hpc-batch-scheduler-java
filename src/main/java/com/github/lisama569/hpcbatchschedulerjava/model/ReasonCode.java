package com.github.lisama569.hpcbatchschedulerjava.model;

/** 请求被拒 / 作业无法运行的可区分原因码。 */
public enum ReasonCode {
    INVALID_REQUEST,
    DUPLICATE_SUBMISSION,
    INSUFFICIENT_CAPACITY,
    QUOTA_EXCEEDED,
    NO_ELIGIBLE_NODE,
    SCHEDULED
}

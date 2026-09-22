package com.github.lisama569.hpcbatchschedulerjava.dto;

/** 设置用户配额请求。 */
public record QuotaRequest(String user,
                           Integer maxRunningJobs,
                           Long maxCpu,
                           Long maxMemoryMb) {
}

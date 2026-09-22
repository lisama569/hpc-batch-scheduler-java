package com.github.lisama569.hpcbatchschedulerjava.domain;

/** 用户配额：最大并发运行作业数、运行作业可占用的 CPU 与内存总量上限。 */
public record UserQuota(String user, int maxRunningJobs, long maxCpu, long maxMemoryMb) {
}

package com.github.lisama569.hpcbatchschedulerjava.web.dto;

/** 单个任务的资源需求。 */
public record TaskRequest(String name, Long cpu, Long memoryMb, Long gpu) {
}

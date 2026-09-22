package com.github.lisama569.hpcbatchschedulerjava.dto;

/** 注册节点请求。 */
public record NodeRequest(String nodeId,
                          Long cpu,
                          Long memoryMb,
                          Long gpu) {
}

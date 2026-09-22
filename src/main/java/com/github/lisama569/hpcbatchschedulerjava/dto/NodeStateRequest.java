package com.github.lisama569.hpcbatchschedulerjava.dto;

/** 修改节点状态请求（HEALTHY / DRAINING / UNAVAILABLE）。 */
public record NodeStateRequest(String state) {
}

package com.github.lisama569.hpcbatchschedulerjava.dto;

import java.util.List;
import java.util.Map;

/** 节点视图：容量、占用、状态与运行作业。 */
public record NodeView(String nodeId,
                       Map<String, Long> capacity,
                       Map<String, Long> used,
                       String state,
                       List<String> runningJobIds) {
}

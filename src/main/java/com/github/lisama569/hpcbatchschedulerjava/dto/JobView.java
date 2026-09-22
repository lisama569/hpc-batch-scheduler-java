package com.github.lisama569.hpcbatchschedulerjava.dto;

import java.util.List;

/** 作业视图（查询/列表用）。 */
public record JobView(String jobId,
                      String idempotencyKey,
                      String user,
                      int priority,
                      int nodeCount,
                      long cpu,
                      long memoryMb,
                      long gpu,
                      String state,
                      List<String> assignedNodeIds,
                      String lastReason,
                      String preemptedByJobId) {
}

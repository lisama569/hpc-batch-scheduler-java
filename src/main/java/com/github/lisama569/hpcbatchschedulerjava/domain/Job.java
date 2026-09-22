package com.github.lisama569.hpcbatchschedulerjava.domain;

import java.util.List;

/** 批处理作业：包含成组调度需求（节点数 + 每节点资源）、优先级与幂等标识。 */
public final class Job {

    private final String jobId;
    private final String idempotencyKey;
    private final String user;
    private final int priority;
    private final int nodeCount;
    private final ResourceSet perNodeResource;
    private final long submittedAtMillis;

    private volatile JobState state;
    private volatile List<String> assignedNodeIds = List.of();
    private volatile String lastReason;
    private volatile String preemptedByJobId;

    public Job(String jobId, String idempotencyKey, String user, int priority,
               int nodeCount, ResourceSet perNodeResource, long submittedAtMillis) {
        this.jobId = jobId;
        this.idempotencyKey = idempotencyKey;
        this.user = user;
        this.priority = priority;
        this.nodeCount = nodeCount;
        this.perNodeResource = perNodeResource;
        this.submittedAtMillis = submittedAtMillis;
        this.state = JobState.QUEUED;
    }

    public String getJobId() {
        return jobId;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getUser() {
        return user;
    }

    public int getPriority() {
        return priority;
    }

    public int getNodeCount() {
        return nodeCount;
    }

    public ResourceSet getPerNodeResource() {
        return perNodeResource;
    }

    public long getSubmittedAtMillis() {
        return submittedAtMillis;
    }

    public JobState getState() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public List<String> getAssignedNodeIds() {
        return assignedNodeIds;
    }

    public void setAssignedNodeIds(List<String> assignedNodeIds) {
        this.assignedNodeIds = assignedNodeIds;
    }

    public String getLastReason() {
        return lastReason;
    }

    public void setLastReason(String lastReason) {
        this.lastReason = lastReason;
    }

    public String getPreemptedByJobId() {
        return preemptedByJobId;
    }

    public void setPreemptedByJobId(String preemptedByJobId) {
        this.preemptedByJobId = preemptedByJobId;
    }
}

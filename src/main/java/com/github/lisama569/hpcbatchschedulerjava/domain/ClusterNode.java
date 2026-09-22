package com.github.lisama569.hpcbatchschedulerjava.domain;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 集群节点：持有容量账本与状态。
 * 所有变更都在 SchedulerService 的同一把锁内发生，故本类不额外加锁。
 */
public final class ClusterNode {

    private final String nodeId;
    private final ResourceSet capacity;
    private ResourceSet used;
    private NodeState state;
    private final Set<String> runningJobIds = new LinkedHashSet<>();

    public ClusterNode(String nodeId, ResourceSet capacity, NodeState state) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        if (capacity == null) {
            throw new IllegalArgumentException("capacity must not be null");
        }
        if (state == null) {
            throw new IllegalArgumentException("state must not be null");
        }
        this.nodeId = nodeId;
        this.capacity = capacity;
        this.used = ResourceSet.ZERO;
        this.state = state;
    }

    public String getNodeId() {
        return nodeId;
    }

    public ResourceSet getCapacity() {
        return capacity;
    }

    public ResourceSet getUsed() {
        return used;
    }

    public NodeState getState() {
        return state;
    }

    public void setState(NodeState state) {
        this.state = state;
    }

    public Set<String> getRunningJobIds() {
        return Collections.unmodifiableSet(runningJobIds);
    }

    /** 节点剩余可用资源（capacity - used）。 */
    public ResourceSet available() {
        ResourceSet rest = ResourceSet.subtractOrNull(capacity, used);
        if (rest == null) {
            // 理论上不可达：出现即说明账本已被破坏。
            throw new IllegalStateException("ledger corrupted on node " + nodeId + ": used " + used + " > capacity " + capacity);
        }
        return rest;
    }

    /**
     * 尝试占用资源。容量不足返回 false 且不产生任何变更（无部分分配）。
     */
    public boolean tryAllocate(String jobId, ResourceSet request) {
        if (!ResourceSet.dominates(available(), request)) {
            return false;
        }
        this.used = ResourceSet.sum(this.used, request);
        this.runningJobIds.add(jobId);
        return true;
    }

    /**
     * 释放资源。释放量超过当前占用即抛出异常，以暴露账本不一致（防止重复回收/泄漏）。
     */
    public void release(String jobId, ResourceSet request) {
        ResourceSet next = ResourceSet.subtractOrNull(this.used, request);
        if (next == null) {
            throw new IllegalStateException(
                    "over-release on node " + nodeId + ": used=" + used + ", release=" + request);
        }
        this.used = next;
        this.runningJobIds.remove(jobId);
    }

    public boolean isSchedulable() {
        return state == NodeState.HEALTHY;
    }
}

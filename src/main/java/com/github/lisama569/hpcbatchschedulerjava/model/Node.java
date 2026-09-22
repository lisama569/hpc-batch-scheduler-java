package com.github.lisama569.hpcbatchschedulerjava.model;

/**
 * 集群节点：容量 + 实际占用 + 状态。
 * 字段可变但仅允许在 {@link com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger}
 * 持有的锁内被修改，外部拿到的快照不代表并发一致性。
 */
public final class Node {

    private final String id;
    private final Resources capacity;
    private Resources used;
    private NodeState state;

    public Node(String id, Resources capacity) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("node id must not be blank");
        }
        if (capacity == null) {
            throw new IllegalArgumentException("node capacity must not be null");
        }
        this.id = id;
        this.capacity = capacity;
        this.used = Resources.empty();
        this.state = NodeState.READY;
    }

    public String id() {
        return id;
    }

    public Resources capacity() {
        return capacity;
    }

    public Resources used() {
        return used;
    }

    /** 仅供账本在锁内调用：设置占用量，调用方保证不超额。 */
    public void setUsed(Resources used) {
        this.used = used;
    }

    public Resources available() {
        return capacity.minus(used);
    }

    public NodeState state() {
        return state;
    }

    public void setState(NodeState state) {
        if (state == null) {
            throw new IllegalArgumentException("node state must not be null");
        }
        this.state = state;
    }

    /** 仅 READY 节点可以接收新落点；UNSCHEDULABLE 与 DRAINING 都不接收。 */
    public boolean acceptsNewWork() {
        return state == NodeState.READY;
    }
}

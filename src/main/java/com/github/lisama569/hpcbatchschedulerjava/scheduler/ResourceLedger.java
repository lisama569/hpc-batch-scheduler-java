package com.github.lisama569.hpcbatchschedulerjava.scheduler;

import com.github.lisama569.hpcbatchschedulerjava.domain.ClusterNode;
import com.github.lisama569.hpcbatchschedulerjava.domain.NodeState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 进程内集群资源账本。
 * 所有读写都要求在 SchedulerService 的同一把锁内进行，因此本类不再加锁。
 */
public final class ResourceLedger {

    private final Map<String, ClusterNode> nodes = new LinkedHashMap<>();

    public ResourceLedger() {
    }

    /** 注册节点；节点已存在返回 false。 */
    public boolean registerNode(String nodeId, ResourceSet capacity) {
        if (nodes.containsKey(nodeId)) {
            return false;
        }
        nodes.put(nodeId, new ClusterNode(nodeId, capacity, NodeState.HEALTHY));
        return true;
    }

    public ClusterNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public List<ClusterNode> allNodes() {
        return new ArrayList<>(nodes.values());
    }

    /** 当前可接收新作业的健康节点（排空中/不可用节点不参与调度）。 */
    public List<ClusterNode> schedulableNodes() {
        return nodes.values().stream().filter(ClusterNode::isSchedulable).toList();
    }

    /** 修改节点状态；节点不存在返回 false。 */
    public boolean setState(String nodeId, NodeState state) {
        ClusterNode node = nodes.get(nodeId);
        if (node == null) {
            return false;
        }
        node.setState(state);
        return true;
    }

    /**
     * 在给定节点集合上原子性地完成成组分配：
     * 全部节点都能满足 perNode 才落账；任一不足则整体回滚，不产生部分分配。
     */
    public boolean allocateGang(List<String> nodeIds, String jobId, ResourceSet perNode) {
        List<ClusterNode> targets = new ArrayList<>(nodeIds.size());
        for (String nodeId : nodeIds) {
            ClusterNode node = nodes.get(nodeId);
            if (node == null || !node.isSchedulable()) {
                return false;
            }
            if (!ResourceSet.dominates(node.available(), perNode)) {
                return false;
            }
            targets.add(node);
        }
        // 先校验全部通过，再统一落账（提交阶段不会再失败）。
        for (ClusterNode node : targets) {
            boolean ok = node.tryAllocate(jobId, perNode);
            if (!ok) {
                // 极端情况下（同锁内不应发生）仍做回滚，保证不出现部分分配。
                for (ClusterNode committed : targets) {
                    if (committed == node) {
                        break;
                    }
                    committed.release(jobId, perNode);
                }
                return false;
            }
        }
        return true;
    }

    /** 回收一个作业在其全部节点上的占用；超额释放会抛出异常以暴露不一致。 */
    public void releaseJob(String jobId, List<String> nodeIds, ResourceSet perNode) {
        for (String nodeId : nodeIds) {
            ClusterNode node = nodes.get(nodeId);
            if (node == null) {
                throw new IllegalStateException("release failed: node vanished " + nodeId);
            }
            node.release(jobId, perNode);
        }
    }

    /** 全集群已用资源汇总（用于守恒校验）。 */
    public ResourceSet totalUsed() {
        ResourceSet total = ResourceSet.ZERO;
        for (ClusterNode node : nodes.values()) {
            total = ResourceSet.sum(total, node.getUsed());
        }
        return total;
    }

    /** 全集群容量汇总。 */
    public ResourceSet totalCapacity() {
        ResourceSet total = ResourceSet.ZERO;
        for (ClusterNode node : nodes.values()) {
            total = ResourceSet.sum(total, node.getCapacity());
        }
        return total;
    }

    /** 账本一致性自检：每个节点 used <= capacity。 */
    public boolean isConsistent() {
        for (ClusterNode node : nodes.values()) {
            if (!ResourceSet.dominates(node.getCapacity(), node.getUsed())) {
                return false;
            }
        }
        return ResourceSet.dominates(totalCapacity(), totalUsed());
    }

    /** 计算 available 相对 need 的逐类缺额（仅包含不足的资源类）。 */
    static Map<String, Long> shortfall(ResourceSet need, ResourceSet available) {
        Map<String, Long> gap = new LinkedHashMap<>();
        for (ResourceType type : ResourceType.values()) {
            long diff = need.get(type) - available.get(type);
            if (diff > 0) {
                gap.put(type.name(), diff);
            }
        }
        return gap;
    }
}

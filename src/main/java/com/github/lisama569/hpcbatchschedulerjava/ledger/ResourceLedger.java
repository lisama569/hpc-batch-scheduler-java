package com.github.lisama569.hpcbatchschedulerjava.ledger;

import com.github.lisama569.hpcbatchschedulerjava.model.Node;
import com.github.lisama569.hpcbatchschedulerjava.model.NodeState;
import com.github.lisama569.hpcbatchschedulerjava.model.ResourceType;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.model.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 进程内集群资源账本：用单机内存账本模拟集群节点。
 *
 * <p>所有读写都在同一把私有锁内完成，从而在“并发提交”这一入口下保证：
 * <ul>
 *   <li>成组分配先聚合校验、再整体提交，绝不出现部分分配；</li>
 *   <li>占用永远满足 {@code 0 <= used <= capacity}，容量守恒、无超额占用；</li>
 *   <li>排空（DRAINING）/不可调度（UNSCHEDULABLE）节点一律不接收新落点。</li>
 * </ul>
 */
public class ResourceLedger {

    private static final Logger log = LoggerFactory.getLogger(ResourceLedger.class);

    private final Object lock = new Object();
    private final Map<String, Node> nodes = new LinkedHashMap<>();

    public ResourceLedger() {
    }

    public void addNode(String nodeId, Resources capacity) {
        synchronized (lock) {
            if (nodes.containsKey(nodeId)) {
                throw new IllegalArgumentException("node already exists: " + nodeId);
            }
            nodes.put(nodeId, new Node(nodeId, capacity));
            log.info("账本: 注册节点 {} 容量={}", nodeId, capacity.describe());
        }
    }

    public void setState(String nodeId, NodeState state) {
        synchronized (lock) {
            requireNode(nodeId).setState(state);
            log.info("账本: 节点 {} 状态 -> {}", nodeId, state);
        }
    }

    public Node getNode(String nodeId) {
        synchronized (lock) {
            return requireNode(nodeId);
        }
    }

    /** 返回节点实时引用；调用方若要读取一致视图，必须放在 {@link #withLock} 内。 */
    public List<Node> snapshotNodes() {
        synchronized (lock) {
            return new ArrayList<>(nodes.values());
        }
    }

    public Map<String, NodeState> nodeStates() {
        synchronized (lock) {
            Map<String, NodeState> result = new LinkedHashMap<>();
            nodes.forEach((id, node) -> result.put(id, node.state()));
            return result;
        }
    }

    public boolean canFit(String nodeId, Resources requirement) {
        synchronized (lock) {
            Node node = requireNode(nodeId);
            boolean fit = node.acceptsNewWork() && node.available().containsAll(requirement);
            log.debug("账本: canFit node={} req={} state={} available={} => {}",
                    nodeId, requirement.describe(), node.state(), node.available().describe(), fit);
            return fit;
        }
    }

    public boolean tryAllocate(String nodeId, Resources requirement) {
        synchronized (lock) {
            Node node = requireNode(nodeId);
            if (!node.acceptsNewWork()) {
                log.info("账本: 拒绝分配 node={} req={} 依据=节点状态{}不接收新作业",
                        nodeId, requirement.describe(), node.state());
                return false;
            }
            if (!node.available().containsAll(requirement)) {
                log.info("账本: 拒绝分配 node={} req={} 依据=空闲{}不足",
                        nodeId, requirement.describe(), node.available().describe());
                return false;
            }
            node.setUsed(node.used().plus(requirement));
            log.info("账本: 分配成功 node={} req={} 占用={}/容量={}",
                    nodeId, requirement.describe(), node.used().describe(), node.capacity().describe());
            return true;
        }
    }

    public void release(String nodeId, Resources requirement) {
        synchronized (lock) {
            Node node = requireNode(nodeId);
            if (!node.capacity().containsAll(node.used().minus(requirement))) {
                throw new IllegalStateException("release would make used negative on node " + nodeId);
            }
            node.setUsed(node.used().minus(requirement));
            log.info("账本: 回收 node={} req={} 占用={}/容量={}",
                    nodeId, requirement.describe(), node.used().describe(), node.capacity().describe());
        }
    }

    /**
     * 在锁内试算一组任务的 first-fit 落点（不修改账本）。
     * 同一作业的多个任务可落到同一节点，但必须逐任务满足容量。
     *
     * @return 可行时返回 taskIndex -> nodeId；不可行返回 null，并向 reasons 追加逐任务依据
     */
    public Map<Integer, String> planPlacement(List<TaskSpec> tasks, List<String> reasons) {
        return planPlacementWithRelease(tasks, Map.of(), reasons);
    }

    /**
     * 带“虚拟释放量”的只读试算：假设各节点额外释放 releasedExtra 中的资源后，
     * 任务能否成组落地。用于抢占规划，不真正修改账本，因此不存在抢占失败后泄漏。
     */
    public Map<Integer, String> planPlacementWithRelease(List<TaskSpec> tasks,
                                                         Map<String, Resources> releasedExtra,
                                                         List<String> reasons) {
        synchronized (lock) {
            List<Node> readyNodes = nodes.values().stream().filter(Node::acceptsNewWork).toList();
            if (readyNodes.isEmpty()) {
                reasons.add("无合格节点：全部节点处于 UNSCHEDULABLE/DRAINING，不接收新作业");
                return null;
            }
            Map<String, Resources> tentativeExtra = new HashMap<>();
            Map<Integer, String> placement = new LinkedHashMap<>();
            for (int i = 0; i < tasks.size(); i++) {
                TaskSpec task = tasks.get(i);
                Node chosen = null;
                for (Node node : readyNodes) {
                    Resources released = releasedExtra.getOrDefault(node.id(), Resources.empty());
                    Resources tentativeUsed = node.used()
                            .plus(tentativeExtra.getOrDefault(node.id(), Resources.empty()))
                            .minus(released);
                    if (node.capacity().containsAll(tentativeUsed.plus(task.requirement()))) {
                        chosen = node;
                        break;
                    }
                }
                if (chosen == null) {
                    reasons.add("任务[" + i + "](" + task.name() + ", 需求=" + task.requirement().describe()
                            + ") 无READY节点可容纳: " + diagnoseTask(task, tentativeExtra, releasedExtra));
                    return null;
                }
                tentativeExtra.merge(chosen.id(), task.requirement(), Resources::plus);
                placement.put(i, chosen.id());
            }
            return placement;
        }
    }

    /**
     * 原子提交成组落点：先聚合每个节点的需求并整体校验（节点存在、READY、容量足够），
     * 全部通过才一次性修改账本；任一不满足则完全不修改，杜绝部分分配。
     */
    public boolean commitAtomic(List<TaskSpec> tasks, Map<Integer, String> placement) {
        synchronized (lock) {
            List<String> failures = new ArrayList<>();
            Map<String, Resources> needPerNode = aggregateNeeds(tasks, placement, failures);
            if (needPerNode == null) {
                log.info("账本: 原子提交被拒绝 依据={}", failures);
                return false;
            }
            for (Map.Entry<String, Resources> entry : needPerNode.entrySet()) {
                Node node = requireNode(entry.getKey());
                if (!node.acceptsNewWork()) {
                    log.info("账本: 原子提交被拒绝 node={} 依据=状态{}不接收新作业", node.id(), node.state());
                    return false;
                }
                if (!node.available().containsAll(entry.getValue())) {
                    log.info("账本: 原子提交被拒绝 node={} 依据=空闲{}不足，需求={}",
                            node.id(), node.available().describe(), entry.getValue().describe());
                    return false;
                }
            }
            for (Map.Entry<String, Resources> entry : needPerNode.entrySet()) {
                Node node = requireNode(entry.getKey());
                node.setUsed(node.used().plus(entry.getValue()));
            }
            log.info("账本: 原子提交成功 落点={} 各节点占用: {}",
                    placement, summarizeUsage(needPerNode.keySet()));
            return true;
        }
    }

    /** 按节点聚合任务需求，并校验落点索引完整、节点存在。 */
    private Map<String, Resources> aggregateNeeds(List<TaskSpec> tasks, Map<Integer, String> placement,
                                                  List<String> failures) {
        if (placement.size() != tasks.size()) {
            failures.add("落点不完整：任务数=" + tasks.size() + " 落点数=" + placement.size());
            return null;
        }
        Map<String, Resources> needPerNode = new LinkedHashMap<>();
        for (int i = 0; i < tasks.size(); i++) {
            String nodeId = placement.get(i);
            if (nodeId == null || !nodes.containsKey(nodeId)) {
                failures.add("任务[" + i + "] 落点缺失或节点不存在: " + nodeId);
                return null;
            }
            needPerNode.merge(nodeId, tasks.get(i).requirement(), Resources::plus);
        }
        return needPerNode;
    }

    private String summarizeUsage(java.util.Set<String> nodeIds) {
        List<String> parts = new ArrayList<>();
        for (String id : nodeIds) {
            Node node = nodes.get(id);
            parts.add(id + "=" + node.used().describe() + "/" + node.capacity().describe());
        }
        return String.join("; ", parts);
    }

    /** 生成某任务在每个节点上无法落地的具体依据（状态或逐资源缺口），计入虚拟释放量。 */
    private String diagnoseTask(TaskSpec task, Map<String, Resources> tentativeExtra,
                                Map<String, Resources> releasedExtra) {
        List<String> parts = new ArrayList<>();
        for (Node node : nodes.values()) {
            if (!node.acceptsNewWork()) {
                parts.add(node.id() + "[状态=" + node.state() + "]");
                continue;
            }
            Resources released = releasedExtra.getOrDefault(node.id(), Resources.empty());
            Resources projectedUsed = node.used()
                    .plus(tentativeExtra.getOrDefault(node.id(), Resources.empty()))
                    .minus(released);
            Resources projectedAvailable = node.capacity().minus(projectedUsed);
            List<String> gaps = new ArrayList<>();
            for (ResourceType type : ResourceType.values()) {
                long need = task.requirement().get(type);
                long avail = projectedAvailable.get(type);
                if (avail < need) {
                    gaps.add(type + "缺" + (need - avail));
                }
            }
            parts.add(node.id() + (gaps.isEmpty() ? "[可容纳]" : "[" + String.join(",", gaps) + "]"));
        }
        return String.join("; ", parts);
    }

    public void releaseAtomic(List<TaskSpec> tasks, Map<Integer, String> placement) {
        synchronized (lock) {
            for (int i = 0; i < tasks.size(); i++) {
                String nodeId = placement.get(i);
                if (nodeId == null) {
                    continue;
                }
                Node node = requireNode(nodeId);
                node.setUsed(node.used().minus(tasks.get(i).requirement()));
            }
            log.info("账本: 原子回收完成 落点={} {}", placement, summarizeUsage(new java.util.TreeSet<>(placement.values())));
        }
    }

    public <T> T withLock(Supplier<T> action) {
        synchronized (lock) {
            return action.get();
        }
    }

    /** 校验账本自身不变量：占用非负且不超容量。返回违例描述（空列表表示守恒）。 */
    public List<String> verifyInvariants() {
        synchronized (lock) {
            List<String> violations = new ArrayList<>();
            for (Node node : nodes.values()) {
                for (ResourceType type : ResourceType.values()) {
                    long used = node.used().get(type);
                    long capacity = node.capacity().get(type);
                    if (used < 0) {
                        violations.add(node.id() + " " + type + " 占用为负: " + used);
                    }
                    if (used > capacity) {
                        violations.add(node.id() + " " + type + " 超额占用: used=" + used + " capacity=" + capacity);
                    }
                }
            }
            return violations;
        }
    }

    private Node requireNode(String nodeId) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("node not found: " + nodeId);
        }
        return node;
    }
}

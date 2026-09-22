package com.github.lisama569.hpcbatchschedulerjava.service;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Job;
import com.github.lisama569.hpcbatchschedulerjava.model.JobState;
import com.github.lisama569.hpcbatchschedulerjava.model.Node;
import com.github.lisama569.hpcbatchschedulerjava.model.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 节点运维入口。
 *
 * <p>DRAINING（排空）/UNSCHEDULABLE 节点不再接收新作业；排空时可选地驱逐其上在跑任务，
 * 被驱逐作业占用的资源整组回收并重新排队（与抢占相同的资源回收路径，保证不泄漏）。
 * 节点恢复为 READY 后重新参与调度。
 */
public class ClusterService {

    private static final Logger log = LoggerFactory.getLogger(ClusterService.class);

    private final ResourceLedger ledger;
    private final JobService jobService;
    private final Object jobLock;

    /**
     * @param jobService 用于在节点状态变化后统一重调度；其内部锁保证与提交路径互斥
     * @param jobLock    与 JobService 共享的调度锁，确保“驱逐回收 + 重新排队”原子
     */
    public ClusterService(ResourceLedger ledger, JobService jobService, Object jobLock) {
        this.ledger = ledger;
        this.jobService = jobService;
        this.jobLock = jobLock;
    }

    public Node markState(String nodeId, NodeState state) {
        ledger.getNode(nodeId); // 存在性校验
        ledger.setState(nodeId, state);
        log.info("集群: 节点 {} -> {}", nodeId, state);
        return ledger.getNode(nodeId);
    }

    /**
     * 排空节点：标记 DRAINING，停止接收新作业。
     *
     * @param evictRunning true 时驱逐该节点上的在跑作业并整组回收资源
     * @return 被驱逐的作业 id 列表
     */
    public List<String> drainNode(String nodeId, boolean evictRunning) {
        ledger.getNode(nodeId);
        List<String> evicted = new ArrayList<>();
        synchronized (jobLock) {
            ledger.setState(nodeId, NodeState.DRAINING);
            if (evictRunning) {
                for (Job job : jobService.listJobs()) {
                    if (job.state() != JobState.RUNNING) {
                        continue;
                    }
                    boolean onNode = job.placement().containsValue(nodeId);
                    if (!onNode) {
                        continue;
                    }
                    ledger.releaseAtomic(job.tasks(), job.placement());
                    job.markPreempted("drain:" + nodeId, List.of(
                            "节点 " + nodeId + " 排空驱逐：占用资源已整组回收，作业重新排队"));
                    evicted.add(job.id());
                    log.info("集群: 排空 {} 驱逐作业 {} 并回收资源", nodeId, job.id());
                }
            }
        }
        if (!evicted.isEmpty() || evictRunning) {
            jobService.schedulePending();
        }
        log.info("集群: 节点 {} 进入 DRAINING，驱逐作业={}", nodeId, evicted);
        return evicted;
    }

    public Node recoverNode(String nodeId) {
        ledger.getNode(nodeId);
        ledger.setState(nodeId, NodeState.READY);
        log.info("集群: 节点 {} 恢复 READY，触发重调度", nodeId);
        jobService.schedulePending();
        return ledger.getNode(nodeId);
    }

    public List<Node> listNodes() {
        return ledger.snapshotNodes();
    }

    public List<String> verifyInvariants() {
        List<String> violations = new ArrayList<>(ledger.verifyInvariants());
        // 交叉校验：在跑作业任务落点占用之和必须等于账本占用
        synchronized (jobLock) {
            Map<String, com.github.lisama569.hpcbatchschedulerjava.model.Resources> expected =
                    new java.util.LinkedHashMap<>();
            for (Job job : jobService.listJobs()) {
                if (job.state() != JobState.RUNNING) {
                    continue;
                }
                for (Map.Entry<Integer, String> e : job.placement().entrySet()) {
                    expected.merge(e.getValue(), job.tasks().get(e.getKey()).requirement(),
                            com.github.lisama569.hpcbatchschedulerjava.model.Resources::plus);
                }
            }
            for (Node node : ledger.snapshotNodes()) {
                var exp = expected.getOrDefault(node.id(),
                        com.github.lisama569.hpcbatchschedulerjava.model.Resources.empty());
                if (!exp.view().equals(node.used().view())) {
                    violations.add("节点 " + node.id() + " 占用不一致: 账本=" + node.used().describe()
                            + " 作业落点聚合=" + exp.describe());
                }
            }
        }
        return violations;
    }
}

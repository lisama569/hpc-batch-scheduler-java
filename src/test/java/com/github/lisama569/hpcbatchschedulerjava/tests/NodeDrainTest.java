package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
import com.github.lisama569.hpcbatchschedulerjava.domain.NodeState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.UserQuota;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.ReasonCode;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionReport;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 节点健康与排空：
 * 排空/不可用节点不再接收新作业；恢复为 HEALTHY 后排队作业可被调度。
 */
class NodeDrainTest {

    private static final Logger log = LoggerFactory.getLogger(NodeDrainTest.class);

    private SchedulerService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SchedulerService();
        scheduler.registerNode("n1", ResourceSet.of(4, 8000, 0));
        scheduler.registerNode("n2", ResourceSet.of(4, 8000, 0));
        scheduler.setQuota(new UserQuota("u", 100, 1000, 10_000_000));
        log.info("[setUp] n1/n2 各 4C8G");
    }

    @Test
    void drainingNodeAcceptsNoNewJobsButRunningJobsKeepResources() {
        // 先占满 n1：4CPU
        SubmissionReport onN1 = scheduler.submit("j-occupy", "u", 5, 1, ResourceSet.of(4, 1000, 0));
        log.info("[输入] j-occupy 4CPU => state={} nodes={}", onN1.jobState(),
                scheduler.getJob(onN1.jobId()).getAssignedNodeIds());
        assertEquals(JobState.RUNNING, onN1.jobState());

        // n1 进入排空：已有运行作业不被驱逐，但不再接收新作业
        scheduler.setNodeState("n1", NodeState.DRAINING);
        log.info("[判定] n1 置为 DRAINING（保留既有运行作业 j-occupy，拒绝新作业）");

        // 成组作业需要 2 个节点同时满足 1CPU；可调度节点只剩 n2（n1 已被占用且排空）
        SubmissionReport r = scheduler.submit("j-after-drain", "u", 5, 2, ResourceSet.of(1, 100, 0));
        log.info("[输入] j-after-drain 需要2节点各1CPU => state={} reason={} basis={}",
                r.jobState(), r.reasonCode(), scheduler.getJob(r.jobId()).getLastReason());
        assertEquals(JobState.QUEUED, r.jobState(), "排空中的 n1 不参与调度，凑不齐 2 个节点");
        assertEquals(JobState.RUNNING, scheduler.getJob(onN1.jobId()).getState(),
                "排空中节点上的既有作业不被驱逐，资源不回收");

        // n1 恢复健康后 tick：n1 仍被 j-occupy 占满，j-after-drain 需要 2 节点，仍无法运行
        scheduler.setNodeState("n1", NodeState.HEALTHY);
        scheduler.scheduleTick();
        assertEquals(JobState.QUEUED, scheduler.getJob(r.jobId()).getState(),
                "n1 恢复但容量仍被占用，成组需求仍不满足");

        // 等既有作业完成，n1 资源完整回收后，成组作业才能启动
        scheduler.completeJob(onN1.jobId());
        assertEquals(JobState.RUNNING, scheduler.getJob(r.jobId()).getState(),
                "资源回收 + n1 恢复后，两个节点同时可用，成组作业启动");
        log.info("[判定] j-occupy 完成释放 n1 后 j-after-drain state={} nodes={}",
                scheduler.getJob(r.jobId()).getState(),
                scheduler.getJob(r.jobId()).getAssignedNodeIds());
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void unavailableNodeExcludedThenResumes() {
        // n1 不可用 => 单节点大需求（需要全部 4C）只能落在 n2；第二个排队
        scheduler.setNodeState("n1", NodeState.UNAVAILABLE);
        SubmissionReport a = scheduler.submit("j-a", "u", 5, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport b = scheduler.submit("j-b", "u", 5, 1, ResourceSet.of(4, 1000, 0));
        log.info("[输入] n1=UNAVAILABLE，连续提交两个 4CPU 作业");
        log.info("[判定] j-a={}/{} j-b={}/{}", a.jobState(), a.reasonCode(), b.jobState(), b.reasonCode());
        assertEquals(JobState.RUNNING, a.jobState());
        assertEquals(JobState.QUEUED, b.jobState());
        assertEquals("n2", scheduler.getJob(a.jobId()).getAssignedNodeIds().get(0),
                "不可用节点不得承载新作业");

        // 恢复 n1 并触发调度，排队作业落到恢复节点
        scheduler.setNodeState("n1", NodeState.HEALTHY);
        int started = scheduler.scheduleTick();
        log.info("[判定] n1 恢复 HEALTHY 后 tick 启动数={} j-b nodes={} state={}",
                started, scheduler.getJob(b.jobId()).getAssignedNodeIds(),
                scheduler.getJob(b.jobId()).getState());
        assertEquals(1, started);
        assertEquals(JobState.RUNNING, scheduler.getJob(b.jobId()).getState());
        assertEquals("n1", scheduler.getJob(b.jobId()).getAssignedNodeIds().get(0));
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void drainAllNodesThenRecoverDrainsQueue() {
        // 两个节点都排空：任何新作业都只能排队，原因是节点数不足/不可调度
        scheduler.setNodeState("n1", NodeState.DRAINING);
        scheduler.setNodeState("n2", NodeState.DRAINING);
        SubmissionReport r = scheduler.submit("j-blocked", "u", 5, 1, ResourceSet.of(1, 100, 0));
        log.info("[输入] 全部节点 DRAINING，提交 1CPU 作业 => state={} reason={} basis={}",
                r.jobState(), r.reasonCode(), scheduler.getJob(r.jobId()).getLastReason());
        assertEquals(JobState.QUEUED, r.jobState());
        assertTrue(r.reasonCode() == ReasonCode.NO_FITTING_NODE
                        || r.reasonCode() == ReasonCode.INSUFFICIENT_CAPACITY,
                "没有可调度节点时必须以容量类原因排队，而不是异常");

        scheduler.setNodeState("n1", NodeState.HEALTHY);
        scheduler.setNodeState("n2", NodeState.HEALTHY);
        scheduler.scheduleTick();
        log.info("[判定] 节点全部恢复后 j-blocked state={}",
                scheduler.getJob(r.jobId()).getState());
        assertEquals(JobState.RUNNING, scheduler.getJob(r.jobId()).getState());
        assertTrue(scheduler.isLedgerConsistent());
    }
}

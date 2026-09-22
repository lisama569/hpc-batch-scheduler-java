package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.domain.Job;
import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 成组调度：多类资源必须在多个节点上同时满足才能启动，否则排队；
 * 完成后资源完整回收，全局资源总量守恒。
 */
class GangSchedulingTest {

    private static final Logger log = LoggerFactory.getLogger(GangSchedulingTest.class);

    private SchedulerService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SchedulerService();
        // 输入：3 个节点，两种机型；单用户给足配额，隔离出纯调度问题。
        scheduler.registerNode("n1", ResourceSet.of(8, 16000, 2));
        scheduler.registerNode("n2", ResourceSet.of(8, 16000, 2));
        scheduler.registerNode("n3", ResourceSet.of(4, 8000, 0));
        scheduler.setQuota(new UserQuota("alice", 100, 100_000, 10_000_000));
        log.info("[setUp] 节点容量: n1=8C16G2GPU n2=8C16G2GPU n3=4C8G0GPU, 配额: alice 宽松");
    }

    @Test
    void gangRunsOnlyWhenAllNodesSatisfyAllResourceDimensions() {
        // 输入：需要 2 个节点，每节点 6 CPU / 10000MB / 1 GPU
        SubmissionReport report = scheduler.submit("gang-1", "alice", 10, 2,
                ResourceSet.of(6, 10000, 1));
        log.info("[输入] key=gang-1 nodes=2 perNode=6CPU/10000MB/1GPU => status={} reason={} jobState={} msg={}",
                report.status(), report.reasonCode(), report.jobState(), report.message());

        assertEquals(SubmissionStatus.ACCEPTED, report.status());
        assertEquals(JobState.RUNNING, report.jobState(),
                "n1、n2 同时满足三类资源，必须成组启动");
        Job job = scheduler.getJob(report.jobId());
        assertEquals(2, job.getAssignedNodeIds().size());

        // 输入：再来一个同样的作业；判定依据：占用后每个节点只剩 2CPU/6000MB/1GPU
        SubmissionReport report2 = scheduler.submit("gang-2", "alice", 10, 2,
                ResourceSet.of(6, 10000, 1));
        Job job2 = scheduler.getJob(report2.jobId());
        log.info("[输入] key=gang-2 nodes=2 perNode=6CPU/10000MB/1GPU => jobState={} reasonCode={} lastReason={}",
                report2.jobState(), report2.reasonCode(), job2.getLastReason());
        assertEquals(JobState.QUEUED, job2.getState(), "无法再凑齐 2 个节点，必须保持排队，不得部分分配");
        assertEquals(ReasonCode.INSUFFICIENT_CAPACITY, report2.reasonCode());
        assertNotNull(job2.getLastReason(), "排队依据必须可解释");

        assertResourceConservation();

        // 第一个作业完成后，资源完整回收，排队作业成组启动
        scheduler.completeJob(job.getJobId());
        scheduler.scheduleTick();
        log.info("[判定] gang-1 完成并 tick 后 gang-2 state={} nodes={}",
                scheduler.getJob(job2.getJobId()).getState(),
                scheduler.getJob(job2.getJobId()).getAssignedNodeIds());
        assertEquals(JobState.RUNNING, scheduler.getJob(job2.getJobId()).getState());
        assertResourceConservation();
    }

    @Test
    void noNodeFitsPerNodeShapeGivesDistinguishableReason() {
        // 输入：每节点要 16 CPU，超过任何单机容量（硬上限），抢占也救不了
        SubmissionReport report = scheduler.submit("gang-huge", "alice", 10, 1,
                ResourceSet.of(16, 1000, 0));
        log.info("[输入] key=gang-huge nodes=1 perNode=16CPU/1000MB => reason={} msg={}",
                report.reasonCode(), report.message());
        assertEquals(SubmissionStatus.ACCEPTED, report.status());
        assertEquals(JobState.QUEUED, scheduler.getJob(report.jobId()).getState());
        assertEquals(ReasonCode.NO_FITTING_NODE, report.reasonCode(),
                "单机容量硬上限不满足必须与临时容量不足区分开");
    }

    private void assertResourceConservation() {
        long usedCpu = 0;
        long usedMem = 0;
        long usedGpu = 0;
        for (Job j : scheduler.listJobs()) {
            if (j.getState() == JobState.RUNNING) {
                usedCpu += j.getPerNodeResource().get(com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType.CPU) * j.getNodeCount();
                usedMem += j.getPerNodeResource().get(com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType.MEMORY_MB) * j.getNodeCount();
                usedGpu += j.getPerNodeResource().get(com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType.GPU) * j.getNodeCount();
            }
        }
        log.info("[守恒校验] 运行作业累计占用: cpu={} mem={} gpu={} ledgerConsistent={}",
                usedCpu, usedMem, usedGpu, scheduler.isLedgerConsistent());
        assertTrue(scheduler.isLedgerConsistent(), "节点容量与实际占用必须一致");
    }
}

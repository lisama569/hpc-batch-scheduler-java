package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.domain.Job;
import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType;
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
 * 抢占与配额：
 * 高优先级作业抢占严格更低优先级作业，被抢占作业资源完整回收；
 * 同优先级/更高优先级不得被抢占；配额超额给出可区分原因。
 */
class PreemptionQuotaTest {

    private static final Logger log = LoggerFactory.getLogger(PreemptionQuotaTest.class);

    private SchedulerService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SchedulerService();
        scheduler.registerNode("n1", ResourceSet.of(8, 16000, 2));
        scheduler.setQuota(new UserQuota("u", 100, 100_000, 10_000_000));
        log.info("[setUp] 单节点 n1=8C16G2GPU");
    }

    @Test
    void highPriorityPreemptsLowerPriorityAndReclaimsFully() {
        // 低优先级作业占满 8CPU
        SubmissionReport low = scheduler.submit("low", "u", 1, 1, ResourceSet.of(8, 1000, 0));
        assertEquals(JobState.RUNNING, low.jobState());

        // 高优先级作业需要 4CPU；普通放置不足 => 应抢占低优先级作业
        SubmissionReport high = scheduler.submit("high", "u", 10, 1, ResourceSet.of(4, 1000, 0));
        Job lowAfter = scheduler.getJob(low.jobId());
        Job highAfter = scheduler.getJob(high.jobId());
        log.info("[输入] low(prio=1,8C) 已运行；提交 high(prio=10,4C)");
        log.info("[判定] high state={} low state={} low.preemptedBy={} low.reason={}",
                highAfter.getState(), lowAfter.getState(),
                lowAfter.getPreemptedByJobId(), lowAfter.getLastReason());

        assertEquals(JobState.RUNNING, highAfter.getState());
        assertEquals(JobState.PREEMPTED, lowAfter.getState());
        assertEquals(high.jobId(), lowAfter.getPreemptedByJobId());
        assertTrue(lowAfter.getAssignedNodeIds().isEmpty(), "被抢占作业的节点分配必须清空");

        long runningCpu = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING)
                .mapToLong(j -> j.getPerNodeResource().get(ResourceType.CPU) * j.getNodeCount())
                .sum();
        log.info("[守恒校验] 抢占后运行中 CPU={}（期望 4，低优先级 8CPU 已完整回收）账本一致={}",
                runningCpu, scheduler.isLedgerConsistent());
        assertEquals(4, runningCpu, "被抢占作业资源须完整回收，不得泄漏或叠加");
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void equalPriorityIsNotPreempted() {
        SubmissionReport first = scheduler.submit("j1", "u", 5, 1, ResourceSet.of(8, 1000, 0));
        SubmissionReport second = scheduler.submit("j2", "u", 5, 1, ResourceSet.of(4, 1000, 0));
        log.info("[输入] j1 prio=5 占8C，j2 prio=5 需4C");
        log.info("[判定] j1={} j2={} reason={} basis={}",
                scheduler.getJob(first.jobId()).getState(),
                scheduler.getJob(second.jobId()).getState(),
                second.reasonCode(), scheduler.getJob(second.jobId()).getLastReason());
        assertEquals(JobState.RUNNING, scheduler.getJob(first.jobId()).getState());
        assertEquals(JobState.QUEUED, scheduler.getJob(second.jobId()).getState(),
                "同优先级不得互相抢占");
        assertEquals(ReasonCode.INSUFFICIENT_CAPACITY, second.reasonCode());
    }

    @Test
    void preemptionThatStillCannotFitDoesNotTouchRunningJobs() {
        // 低优先级作业占 2CPU；高优先级作业需要单机 16CPU，超过节点容量上限
        SubmissionReport low = scheduler.submit("low", "u", 1, 1, ResourceSet.of(2, 1000, 0));
        SubmissionReport huge = scheduler.submit("huge", "u", 10, 1, ResourceSet.of(16, 1000, 0));
        log.info("[输入] low=2C 运行；huge=16C（节点容量仅 8C）");
        log.info("[判定] huge={} reason={} low 仍运行={}",
                huge.jobState(), huge.reasonCode(),
                scheduler.getJob(low.jobId()).getState());
        assertEquals(JobState.QUEUED, scheduler.getJob(huge.jobId()).getState());
        assertEquals(ReasonCode.NO_FITTING_NODE, huge.reasonCode());
        assertEquals(JobState.RUNNING, scheduler.getJob(low.jobId()).getState(),
                "抢占无意义时不得误杀运行作业");
    }

    @Test
    void quotaLimitsGiveExplicitReasons() {
        SchedulerService svc = new SchedulerService();
        svc.registerNode("n1", ResourceSet.of(64, 128_000, 8));
        // 最多 2 个运行作业、6 CPU
        svc.setQuota(new UserQuota("tiny", 2, 6, 128_000));

        SubmissionReport r1 = svc.submit("q1", "tiny", 5, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport r2 = svc.submit("q2", "tiny", 5, 1, ResourceSet.of(4, 1000, 0));
        log.info("[输入] tiny 配额 maxJobs=2/maxCpu=6，提交两个 4C 作业");
        log.info("[判定] q2 state={} reason={} msg={}", r2.jobState(), r2.reasonCode(), r2.message());
        assertEquals(SubmissionStatus.ACCEPTED, r2.status());
        assertEquals(JobState.QUEUED, svc.getJob(r2.jobId()).getState());
        assertEquals(ReasonCode.QUOTA_CPU_LIMIT, r2.reasonCode(),
                "4+4=8 > 6 CPU，必须给出 CPU 配额超额的可解释原因");
        assertNotNull(svc.getJob(r2.jobId()).getLastReason());

        // 释放后，配额恢复，排队作业启动
        svc.completeJob(r1.jobId());
        svc.scheduleTick();
        assertEquals(JobState.RUNNING, svc.getJob(r2.jobId()).getState(),
                "配额释放后排队作业可启动");
        log.info("[判定] q1 完成释放配额后 q2 state={} 账本一致={}",
                svc.getJob(r2.jobId()).getState(), svc.isLedgerConsistent());
    }
}

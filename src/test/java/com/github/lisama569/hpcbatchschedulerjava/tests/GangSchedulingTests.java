package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Job;
import com.github.lisama569.hpcbatchschedulerjava.model.JobState;
import com.github.lisama569.hpcbatchschedulerjava.model.ReasonCode;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.model.TaskSpec;
import com.github.lisama569.hpcbatchschedulerjava.quota.QuotaManager;
import com.github.lisama569.hpcbatchschedulerjava.service.ClusterService;
import com.github.lisama569.hpcbatchschedulerjava.service.JobService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 成组调度：同时满足才启动，否则排队；配额不足给出逐类依据。 */
class GangSchedulingTests {

    private static final Logger log = LoggerFactory.getLogger(GangSchedulingTests.class);

    private record Harness(ResourceLedger ledger, JobService jobs, ClusterService cluster, QuotaManager quota) {
    }

    private Harness harness() {
        ResourceLedger ledger = new ResourceLedger();
        ledger.addNode("n1", Resources.of(4, 8192, 1));
        ledger.addNode("n2", Resources.of(4, 8192, 1));
        QuotaManager quota = new QuotaManager();
        JobService jobs = new JobService(ledger, quota);
        ClusterService cluster = new ClusterService(ledger, jobs, jobs.schedulingLock());
        return new Harness(ledger, jobs, cluster, quota);
    }

    @Test
    void schedulesOnlyWhenAllTasksFit() {
        Harness h = harness();
        List<TaskSpec> tasks = List.of(
                new TaskSpec("t0", Resources.of(4, 1024, 1)),
                new TaskSpec("t1", Resources.of(4, 1024, 1)));
        log.info("[输入] 成组作业 2 任务，每任务占满一节点 cpu=4/gpu=1");
        Job job = h.jobs().submit("g1", "u1", 0, tasks);
        log.info("[判定] 状态={} 落点={} 依据={}", job.state(), job.placement(), job.decisionLog());
        assertEquals(JobState.RUNNING, job.state());
        assertEquals(2, job.placement().size());
        assertTrue(h.cluster().verifyInvariants().isEmpty());
    }

    @Test
    void keepsJobQueuedAndExplainsShortfallWhenGangDoesNotFit() {
        Harness h = harness();
        // 每节点只有 1 gpu，两任务各需 1 gpu 本来可行；再加第三个需 gpu 的任务 -> 不可行
        List<TaskSpec> tasks = List.of(
                new TaskSpec("t0", Resources.of(1, 1024, 1)),
                new TaskSpec("t1", Resources.of(1, 1024, 1)),
                new TaskSpec("t2", Resources.of(1, 1024, 1)));
        log.info("[输入] 成组作业 3 任务各需 gpu=1，集群仅 2 gpu");
        Job job = h.jobs().submit("g2", "u1", 0, tasks);
        log.info("[判定] 状态={} 原因={} 日志={}", job.state(), job.lastReason(), job.decisionLog());
        assertEquals(JobState.QUEUED, job.state());
        assertEquals(ReasonCode.INSUFFICIENT_CAPACITY, job.lastReason());
        assertTrue(job.decisionLog().stream().anyMatch(s -> s.contains("GPU")), "必须给出 GPU 缺口依据");
        assertTrue(h.ledger().verifyInvariants().isEmpty(), "排队作业不得占用任何资源");
    }

    @Test
    void rejectsOverQuotaWithPerResourceReason() {
        Harness h = harness();
        h.quota().setQuota("u1", Resources.of(2, 4096, 0));
        log.info("[输入] 配额 cpu=2/mem=4096/gpu=0，作业申请 cpu=4/mem=1024");
        Job job = h.jobs().submit("g3", "u1", 0,
                List.of(new TaskSpec("t0", Resources.of(4, 1024, 0))));
        log.info("[判定] 状态={} 原因={} 日志={}", job.state(), job.lastReason(), job.decisionLog());
        assertEquals(JobState.QUEUED, job.state());
        assertEquals(ReasonCode.QUOTA_EXCEEDED, job.lastReason(), "超额原因必须与容量不足可区分");
        assertTrue(job.decisionLog().stream().anyMatch(s -> s.contains("CPU") && s.contains("超过配额")));
        assertTrue(h.ledger().verifyInvariants().isEmpty(), "超额被准入拒绝，不占用资源");
    }

    @Test
    void schedulesFreedTasksOnCompletion() {
        Harness h = harness();
        Job big = h.jobs().submit("g4", "u1", 0,
                List.of(new TaskSpec("t0", Resources.of(4, 1024, 1)),
                        new TaskSpec("t1", Resources.of(4, 1024, 1))));
        assertEquals(JobState.RUNNING, big.state());
        Job waiter = h.jobs().submit("g5", "u2", 0,
                List.of(new TaskSpec("t0", Resources.of(3, 1024, 1))));
        log.info("[输入] 大作业占满两节点，等待作业 g5 需要 cpu=3/gpu=1 => 排队");
        assertEquals(JobState.QUEUED, waiter.state());
        h.jobs().complete(big.id());
        Job waiterAfter = h.jobs().findJob(waiter.id()).orElseThrow();
        log.info("[判定] 大作业完成回收后，g5 状态={} 落点={}", waiterAfter.state(), waiterAfter.placement());
        assertEquals(JobState.RUNNING, waiterAfter.state());
        assertTrue(h.cluster().verifyInvariants().isEmpty());
    }
}

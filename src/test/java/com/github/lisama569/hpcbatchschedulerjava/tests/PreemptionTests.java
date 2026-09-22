package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Job;
import com.github.lisama569.hpcbatchschedulerjava.model.JobState;
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

/** 抢占：仅低优先级可被抢占，资源完整回收，被抢占作业重新排队。 */
class PreemptionTests {

    private static final Logger log = LoggerFactory.getLogger(PreemptionTests.class);

    private record Harness(ResourceLedger ledger, JobService jobs, ClusterService cluster) {
    }

    private Harness harness() {
        ResourceLedger ledger = new ResourceLedger();
        ledger.addNode("n1", Resources.of(4, 8192, 1));
        ledger.addNode("n2", Resources.of(4, 8192, 1));
        QuotaManager quota = new QuotaManager();
        JobService jobs = new JobService(ledger, quota);
        ClusterService cluster = new ClusterService(ledger, jobs, jobs.schedulingLock());
        return new Harness(ledger, jobs, cluster);
    }

    @Test
    void higherPriorityJobPreemptsLowerPriorityJob() {
        Harness h = harness();
        Job low = h.jobs().submit("p-low", "u1", 1,
                List.of(new TaskSpec("t0", Resources.of(4, 1024, 1)),
                        new TaskSpec("t1", Resources.of(4, 1024, 1))));
        log.info("[输入] 低优先级(priority=1)作业 {} 占满两节点", low.id());
        assertEquals(JobState.RUNNING, low.state());

        Job high = h.jobs().submit("p-high", "u2", 10,
                List.of(new TaskSpec("t0", Resources.of(4, 1024, 1))));
        Job lowAfter = h.jobs().findJob(low.id()).orElseThrow();
        log.info("[判定] 高优先级作业 {} 状态={} 落点={}；低优先级作业状态={} 被抢占次数={} 依据={}",
                high.id(), high.state(), high.placement(), lowAfter.state(), lowAfter.preemptCount(),
                lowAfter.decisionLog());
        assertEquals(JobState.RUNNING, high.state(), "高优先级作业应抢占后启动");
        assertEquals(JobState.QUEUED, lowAfter.state(), "被抢占作业应重新排队");
        assertEquals(1, lowAfter.preemptCount());
        assertEquals(high.id(), lowAfter.lastPreemptedByJobId());
        assertTrue(h.cluster().verifyInvariants().isEmpty(),
                "抢占必须完整回收资源: " + h.cluster().verifyInvariants());
    }

    @Test
    void neverPreemptsEqualOrHigherPriority() {
        Harness h = harness();
        Job existing = h.jobs().submit("p-equal-1", "u1", 5,
                List.of(new TaskSpec("t0", Resources.of(4, 1024, 1)),
                        new TaskSpec("t1", Resources.of(4, 1024, 1))));
        Job contender = h.jobs().submit("p-equal-2", "u2", 5,
                List.of(new TaskSpec("t0", Resources.of(4, 1024, 1))));
        log.info("[输入] 两个 priority=5 作业竞争，集群只够一个成组运行");
        assertEquals(JobState.RUNNING, existing.state());
        log.info("[判定] 后到作业状态={} 原因={} 日志={}", contender.state(), contender.lastReason(),
                contender.decisionLog());
        assertEquals(JobState.QUEUED, contender.state(), "同级优先级不得互相抢占");
        assertEquals(0, existing.preemptCount(), "既有作业不得被同级抢占");
        assertTrue(h.cluster().verifyInvariants().isEmpty());
    }

    @Test
    void doesNotPreemptIfGangStillCannotFitAfterwards() {
        Harness h = harness();
        // 低优先级作业只占用 cpu=1 x2 节点；高优先级成组作业每任务要 gpu=2，任何节点都放不下
        Job low = h.jobs().submit("p-low2", "u1", 1,
                List.of(new TaskSpec("t0", Resources.of(1, 1024, 0)),
                        new TaskSpec("t1", Resources.of(1, 1024, 0))));
        assertEquals(JobState.RUNNING, low.state());
        Job huge = h.jobs().submit("p-huge", "u2", 10,
                List.of(new TaskSpec("t0", Resources.of(1, 1024, 2))));
        log.info("[输入] 即使抢占低优先级作业，单节点 gpu=1 仍放不下需 gpu=2 的任务");
        log.info("[判定] 大作业状态={} 原因={}；低优先级作业抢占次数={}（期望 0，不白抢）",
                huge.state(), huge.lastReason(), low.preemptCount());
        assertEquals(JobState.QUEUED, huge.state());
        assertEquals(0, h.jobs().findJob(low.id()).orElseThrow().preemptCount(),
                "抢占后仍不能成组落地时不得抢占任何作业");
        assertEquals(JobState.RUNNING, h.jobs().findJob(low.id()).orElseThrow().state());
        assertTrue(h.cluster().verifyInvariants().isEmpty());
    }
}

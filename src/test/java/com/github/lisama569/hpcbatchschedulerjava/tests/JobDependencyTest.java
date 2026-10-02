package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.domain.Job;
import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.UserQuota;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.ReasonCode;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.ScheduleException;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionReport;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 作业依赖编排：
 * 等待前置不占资源/不计配额/不误报容量不足、成功后自动解锁、取消与抢占的失败结局向下传播、
 * 成环与引用不存在作业在提交时可区分拒绝、并发解锁顺序确定且账本守恒。
 */
class JobDependencyTest {

    private static final Logger log = LoggerFactory.getLogger(JobDependencyTest.class);

    private SchedulerService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SchedulerService();
        scheduler.setQuota(new UserQuota("alice", 100, 10_000, 1_000_000));
        log.info("[setUp] alice 配额宽松，节点按用例各自注册");
    }

    @Test
    void waitingOnDependencyHoldsNoResourceNoQuotaAndExplainsReason() {
        scheduler.registerNode("n1", ResourceSet.of(4, 8000, 0));
        scheduler.registerNode("n2", ResourceSet.of(4, 8000, 0));
        scheduler.setQuota(new UserQuota("alice", 2, 10_000, 1_000_000));
        log.info("[输入] n1/n2 各 4CPU；alice maxRunningJobs=2");

        SubmissionReport a = scheduler.submit("key-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        log.info("[输入] A=key-a 4CPU => state={}", a.jobState());

        SubmissionReport b = scheduler.submit("key-b", "alice", 5, 1, ResourceSet.of(1, 1000, 0),
                List.of(a.jobId()));
        Job bJob = scheduler.getJob(b.jobId());
        log.info("[输入] B=key-b 依赖 A => state={} reason={}", b.jobState(), bJob.getLastReason());

        assertSame(JobState.WAITING, bJob.getState(), "前置未完成时后继必须处于 WAITING");
        assertEquals(ReasonCode.WAITING_ON_DEPENDENCIES, b.reasonCode());
        assertTrue(bJob.getAssignedNodeIds().isEmpty(), "等待期间不得占用节点");
        assertTrue(bJob.getLastReason().contains(a.jobId()), "排队原因必须能看出在等哪个作业");
        assertFalse(bJob.getLastReason().contains("INSUFFICIENT_CAPACITY"),
                "等待依赖不得误报为容量不足");

        // 配额只计 RUNNING：B 等待中不计入，alice 还能再跑一个（上限 2）。
        SubmissionReport c = scheduler.submit("key-c", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        log.info("[判定] B 等待中再提交 C => state={}（若 B 计配额则 C 应被 QUOTA_JOB_LIMIT 拦截）", c.jobState());
        assertSame(JobState.RUNNING, c.jobState(), "WAITING 作业不得计入用户配额占用");
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void successfulDependencyAutoUnlocksWithoutManualTick() {
        scheduler.registerNode("n1", ResourceSet.of(8, 8000, 0));
        SubmissionReport a = scheduler.submit("key-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport b = scheduler.submit("key-b", "alice", 5, 1, ResourceSet.of(4, 1000, 0),
                List.of(a.jobId()));
        log.info("[输入] A 运行中，B 依赖 A 等待中；complete(A) 不手动 tick");

        scheduler.completeJob(a.jobId());
        Job bJob = scheduler.getJob(b.jobId());
        log.info("[判定] A 成功后 B => state={} nodes={}", bJob.getState(), bJob.getAssignedNodeIds());
        assertSame(JobState.RUNNING, bJob.getState(), "前置成功后后继必须被自动重新评估并直接启动");
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void chainDependenciesUnlockInSequence() {
        scheduler.registerNode("n1", ResourceSet.of(4, 8000, 0));
        SubmissionReport a = scheduler.submit("key-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport b = scheduler.submit("key-b", "alice", 5, 1, ResourceSet.of(4, 1000, 0),
                List.of(a.jobId()));
        SubmissionReport c = scheduler.submit("key-c", "alice", 5, 1, ResourceSet.of(4, 1000, 0),
                List.of(b.jobId()));
        log.info("[输入] 链式 A<-B<-C，单节点串行；A={} B={} C={}", a.jobId(), b.jobId(), c.jobId());

        scheduler.completeJob(a.jobId());
        assertSame(JobState.RUNNING, scheduler.getJob(b.jobId()).getState(), "A 完成后 B 解锁");
        assertSame(JobState.WAITING, scheduler.getJob(c.jobId()).getState(), "B 未完成的 C 继续等待");
        log.info("[判定] A 完成 => B=RUNNING, C=WAITING reason={}", scheduler.getJob(c.jobId()).getLastReason());

        scheduler.completeJob(b.jobId());
        assertSame(JobState.RUNNING, scheduler.getJob(c.jobId()).getState(), "B 完成后 C 解锁");
        log.info("[判定] B 完成 => C=RUNNING，链式依赖逐级自动解锁");
    }

    @Test
    void cancelOfDependencyPropagatesFailureTerminally() {
        scheduler.registerNode("n1", ResourceSet.of(8, 8000, 0));
        SubmissionReport a = scheduler.submit("key-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport b = scheduler.submit("key-b", "alice", 5, 1, ResourceSet.of(1, 1000, 0),
                List.of(a.jobId()));
        SubmissionReport c = scheduler.submit("key-c", "alice", 5, 1, ResourceSet.of(1, 1000, 0),
                List.of(b.jobId()));
        log.info("[输入] A 运行中，B 等 A，C 等 B；cancel(A)");

        scheduler.cancelJob(a.jobId());
        Job bJob = scheduler.getJob(b.jobId());
        Job cJob = scheduler.getJob(c.jobId());
        log.info("[判定] B => state={} reason={}", bJob.getState(), bJob.getLastReason());
        log.info("[判定] C => state={} reason={}（级联）", cJob.getState(), cJob.getLastReason());

        assertSame(JobState.DEPENDENCY_FAILED, bJob.getState(), "前置被取消，后继必须落可区分终态");
        assertTrue(bJob.getLastReason().contains("DEPENDENCY_FAILED")
                && bJob.getLastReason().contains(a.jobId()), "终态理由必须指明失败前置");
        assertSame(JobState.DEPENDENCY_FAILED, cJob.getState(), "失败必须级联，不得无声悬挂");

        ScheduleException ex = null;
        try {
            scheduler.cancelJob(b.jobId());
        } catch (ScheduleException e) {
            ex = e;
        }
        assertEquals(ReasonCode.JOB_STATE_CONFLICT, ex == null ? null : ex.getReasonCode(),
                "DEPENDENCY_FAILED 是终态，再取消必须报状态冲突");
        log.info("[判定] 对终态 B 再取消 => JOB_STATE_CONFLICT");
    }

    @Test
    void preemptionOfDependencyPropagatesFailure() {
        scheduler.registerNode("n1", ResourceSet.of(4, 8000, 0));
        SubmissionReport low = scheduler.submit("key-low", "alice", 1, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport dependent = scheduler.submit("key-dep", "alice", 1, 1, ResourceSet.of(1, 1000, 0),
                List.of(low.jobId()));
        log.info("[输入] 低优先级 A 占满节点，B 等 A；提交高优先级 C 触发抢占");

        SubmissionReport high = scheduler.submit("key-high", "alice", 9, 1, ResourceSet.of(4, 1000, 0));
        Job lowJob = scheduler.getJob(low.jobId());
        Job depJob = scheduler.getJob(dependent.jobId());
        log.info("[判定] C => state={}；A => state={}；B => state={} reason={}",
                high.jobState(), lowJob.getState(), depJob.getState(), depJob.getLastReason());

        assertSame(JobState.RUNNING, high.jobState());
        assertSame(JobState.PREEMPTED, lowJob.getState(), "既有抢占行为不得被破坏");
        assertSame(JobState.DEPENDENCY_FAILED, depJob.getState(), "前置被抢占，后继必须落失败终态");
        assertTrue(depJob.getLastReason().contains(low.jobId()));
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void unknownAndCyclicDependenciesAreRejectedWithDistinctReasons() {
        scheduler.registerNode("n1", ResourceSet.of(4, 8000, 0));
        SubmissionReport a = scheduler.submit("key-a", "alice", 5, 1, ResourceSet.of(1, 1000, 0));

        SubmissionReport unknown = scheduler.submit("key-x", "alice", 5, 1, ResourceSet.of(1, 1000, 0),
                List.of("job-ghost"));
        log.info("[输入] 依赖不存在的 job-ghost => status={} reason={} msg={}",
                unknown.status(), unknown.reasonCode(), unknown.message());
        assertSame(SubmissionStatus.REJECTED, unknown.status());
        assertEquals(ReasonCode.DEPENDENCY_NOT_FOUND, unknown.reasonCode());

        SubmissionReport selfCycle = scheduler.submit("key-self", "alice", 5, 1, ResourceSet.of(1, 1000, 0),
                List.of("key-self"));
        log.info("[输入] 通过自身幂等键自引用 => status={} reason={} msg={}",
                selfCycle.status(), selfCycle.reasonCode(), selfCycle.message());
        assertSame(SubmissionStatus.REJECTED, selfCycle.status());
        assertEquals(ReasonCode.DEPENDENCY_CYCLE, selfCycle.reasonCode());

        assertEquals(1, scheduler.listJobs().size(), "被拒的提交不得入队");
        assertTrue(scheduler.getJob(a.jobId()).getDependsOnJobIds().isEmpty());
        log.info("[判定] DEPENDENCY_NOT_FOUND 与 DEPENDENCY_CYCLE 可区分，且均不产生作业实例");
    }

    @Test
    void simultaneousUnlocksStartInDeterministicPriorityOrder() {
        scheduler.registerNode("n1", ResourceSet.of(4, 8000, 0));
        SubmissionReport a = scheduler.submit("key-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport b = scheduler.submit("key-b", "alice", 5, 1, ResourceSet.of(4, 1000, 0),
                List.of(a.jobId()));
        SubmissionReport c = scheduler.submit("key-c", "alice", 9, 1, ResourceSet.of(4, 1000, 0),
                List.of(a.jobId()));
        SubmissionReport d = scheduler.submit("key-d", "alice", 1, 1, ResourceSet.of(4, 1000, 0),
                List.of(a.jobId()));
        log.info("[输入] 单节点串行；B(p5)/C(p9)/D(p1) 同时等 A，complete(A) 同时解锁三个");

        scheduler.completeJob(a.jobId());
        log.info("[判定] 第 1 轮：B={} C={} D={}（容量只够 1 个，优先级最高者先上）",
                scheduler.getJob(b.jobId()).getState(), scheduler.getJob(c.jobId()).getState(),
                scheduler.getJob(d.jobId()).getState());
        assertSame(JobState.RUNNING, scheduler.getJob(c.jobId()).getState(), "优先级 9 必须先上");
        assertSame(JobState.QUEUED, scheduler.getJob(b.jobId()).getState());
        assertSame(JobState.QUEUED, scheduler.getJob(d.jobId()).getState());

        scheduler.completeJob(c.jobId());
        assertSame(JobState.RUNNING, scheduler.getJob(b.jobId()).getState(), "其次优先级 5");
        scheduler.completeJob(b.jobId());
        assertSame(JobState.RUNNING, scheduler.getJob(d.jobId()).getState(), "最后优先级 1");
        log.info("[判定] 解锁顺序 C(p9) -> B(p5) -> D(p1)，稳定可解释；账本一致={}",
                scheduler.isLedgerConsistent());
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void duplicateSubmissionWithDependenciesKeepsSingleInstanceAndSingleEdge() {
        scheduler.registerNode("n1", ResourceSet.of(8, 8000, 0));
        SubmissionReport a = scheduler.submit("key-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        List<String> deps = List.of(a.jobId());

        SubmissionReport first = scheduler.submit("key-b", "alice", 5, 1, ResourceSet.of(1, 1000, 0), deps);
        SubmissionReport dup = scheduler.submit("key-b", "alice", 5, 1, ResourceSet.of(1, 1000, 0), deps);
        log.info("[输入] 同键同依赖重复提交 => status={} jobId={}", dup.status(), dup.jobId());

        assertSame(SubmissionStatus.DUPLICATE, dup.status());
        assertEquals(first.jobId(), dup.jobId());
        assertEquals(2, scheduler.listJobs().size(), "重复提交不得产生第二个实例");
        assertEquals(List.of(a.jobId()), scheduler.getJob(first.jobId()).getDependsOnJobIds(),
                "重复提交不得多挂依赖边");

        SubmissionReport conflict = scheduler.submit("key-b", "alice", 5, 1, ResourceSet.of(1, 1000, 0),
                List.of());
        log.info("[判定] 同键但依赖不同 => status={} reason={}", conflict.status(), conflict.reasonCode());
        assertSame(SubmissionStatus.REJECTED, conflict.status());
        assertEquals(ReasonCode.IDEMPOTENCY_KEY_CONFLICT, conflict.reasonCode(),
                "依赖列表属于幂等内容，不一致必须按冲突拒绝");
    }

    @Test
    void dependencyOnAlreadyTerminalJobResolvesImmediately() {
        scheduler.registerNode("n1", ResourceSet.of(4, 8000, 0));
        SubmissionReport done = scheduler.submit("key-done", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        scheduler.completeJob(done.jobId());
        SubmissionReport cancelled = scheduler.submit("key-cx", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        scheduler.cancelJob(cancelled.jobId());
        log.info("[输入] 前置已完成 key-done={}；前置已取消 key-cx={}", done.jobId(), cancelled.jobId());

        SubmissionReport afterDone = scheduler.submit("key-ok", "alice", 5, 1, ResourceSet.of(4, 1000, 0),
                List.of(done.jobId()));
        log.info("[判定] 依赖已 COMPLETED 作业 => state={}（应立即调度）", afterDone.jobState());
        assertSame(JobState.RUNNING, afterDone.jobState());

        SubmissionReport afterCancel = scheduler.submit("key-bad", "alice", 5, 1, ResourceSet.of(1, 1000, 0),
                List.of(cancelled.jobId()));
        log.info("[判定] 依赖已 CANCELLED 作业 => state={} reason={}",
                afterCancel.jobState(), afterCancel.message());
        assertSame(JobState.DEPENDENCY_FAILED, afterCancel.jobState(),
                "提交时前置已是失败终态，必须立即落 DEPENDENCY_FAILED 而非悬挂");
        assertEquals(ReasonCode.DEPENDENCY_FAILED, afterCancel.reasonCode());
    }

    @Test
    void concurrentSubmitAndUnlockKeepsLedgerConsistent() throws InterruptedException {
        scheduler.registerNode("n1", ResourceSet.of(16, 8000, 0));
        scheduler.registerNode("n2", ResourceSet.of(16, 8000, 0));
        SubmissionReport a = scheduler.submit("key-a", "alice", 5, 1, ResourceSet.of(1, 100, 0));
        log.info("[输入] 2 节点各 16CPU；A 运行中；16 线程并发提交依赖 A 的作业，同时主线程 complete(A)");

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        List<SubmissionReport> reports = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    SubmissionReport r = scheduler.submit("key-dep-" + idx, "alice", 5, 1,
                            ResourceSet.of(1, 100, 0), List.of(a.jobId()));
                    synchronized (reports) {
                        reports.add(r);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        scheduler.completeJob(a.jobId());
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        pool.shutdown();

        long running = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING).count();
        long waiting = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.WAITING).count();
        log.info("[判定] 并发提交+并发解锁后：RUNNING={} WAITING={}（提交与完成竞态下两者合计应为 16）",
                running, waiting);
        assertEquals(threads, running + waiting, "每个并发提交必须恰好产生一个作业实例");
        assertEquals(threads, reports.size());

        scheduler.scheduleTick();
        long runningAfter = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING).count();
        long usedCpu = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING)
                .mapToLong(j -> j.getPerNodeResource().get(com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType.CPU)
                        * j.getNodeCount())
                .sum();
        log.info("[守恒校验] tick 后 RUNNING={} 运行视角 CPU={} 账本一致={}",
                runningAfter, usedCpu, scheduler.isLedgerConsistent());
        assertEquals(threads, runningAfter, "容量充足时全部后继最终都必须启动");
        assertEquals(threads, usedCpu, "不超分、不少分");
        assertTrue(scheduler.isLedgerConsistent());
    }
}

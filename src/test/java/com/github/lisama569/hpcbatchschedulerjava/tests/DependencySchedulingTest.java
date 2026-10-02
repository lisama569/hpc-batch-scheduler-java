package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.domain.ClusterNode;
import com.github.lisama569.hpcbatchschedulerjava.domain.Job;
import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType;
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
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 作业依赖：等待前置不占资源/不计配额、前置成功自动解锁、
 * 取消/抢占的失败结局沿依赖链传递、成环与引用不存在在提交时拒绝、
 * 批量/并发解锁下顺序确定且账本守恒。
 */
class DependencySchedulingTest {

    private static final Logger log = LoggerFactory.getLogger(DependencySchedulingTest.class);

    private SchedulerService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SchedulerService();
        // 输入：2 个节点各 8C/16G/2GPU；alice/bob 配额宽松，隔离出纯依赖问题。
        scheduler.registerNode("n1", ResourceSet.of(8, 16000, 2));
        scheduler.registerNode("n2", ResourceSet.of(8, 16000, 2));
        scheduler.setQuota(new UserQuota("alice", 100, 100_000, 10_000_000));
        scheduler.setQuota(new UserQuota("bob", 100, 100_000, 10_000_000));
        log.info("[setUp] 节点: n1=n2=8C16G2GPU, 配额: alice/bob 宽松");
    }

    @Test
    void waitingOnPredecessorHoldsNoResourceAndNoQuota() {
        // 把 alice 运行数配额压到 1：若等待中的作业计入配额，B 会被配额拦住
        scheduler.setQuota(new UserQuota("alice", 1, 100_000, 10_000_000));
        SubmissionReport a = scheduler.submit("dep-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        assertEquals(JobState.RUNNING, a.jobState());
        String aId = a.jobId();

        SubmissionReport b = scheduler.submit("dep-b", "alice", 5, 1,
                ResourceSet.of(4, 1000, 0), List.of(aId));
        Job bJob = scheduler.getJob(b.jobId());
        log.info("[输入] B dependsOn=[{}] => status={} reason={} state={} lastReason={}",
                aId, b.status(), b.reasonCode(), b.jobState(), bJob.getLastReason());

        assertEquals(SubmissionStatus.ACCEPTED, b.status());
        assertEquals(JobState.WAITING, bJob.getState(), "前置未完成时必须处于 WAITING 而非 QUEUED");
        assertEquals(ReasonCode.WAITING_ON_DEPENDENCIES, b.reasonCode());
        assertTrue(bJob.getLastReason().contains(aId), "排队原因必须能一眼看出在等哪个作业: " + bJob.getLastReason());
        assertTrue(bJob.getAssignedNodeIds().isEmpty(), "等待期间不得占用/预留任何节点");

        long usedCpu = totalUsedCpu();
        log.info("[判定] 等待期间集群已用 CPU={}（仅 A 的 4），ledgerConsistent={}",
                usedCpu, scheduler.isLedgerConsistent());
        assertEquals(4, usedCpu, "等待中的 B 不得占用资源");
        assertFalse(bJob.getLastReason().contains("QUOTA"),
                "配额已满（maxRunningJobs=1）时 B 也不许被误报成配额/容量原因: " + bJob.getLastReason());
    }

    @Test
    void successUnlocksDependentsAutomaticallyWithoutManualTick() {
        SubmissionReport a = scheduler.submit("dep-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport b = scheduler.submit("dep-b", "alice", 5, 1,
                ResourceSet.of(4, 1000, 0), List.of(a.jobId()));
        assertEquals(JobState.WAITING, scheduler.getJob(b.jobId()).getState());

        // 前置成功完成：不调用 scheduleTick，后继必须被自动评估并直接启动
        scheduler.completeJob(a.jobId());
        Job bJob = scheduler.getJob(b.jobId());
        log.info("[判定] A 完成后（无手动 tick）B state={} nodes={} ledgerConsistent={}",
                bJob.getState(), bJob.getAssignedNodeIds(), scheduler.isLedgerConsistent());
        assertEquals(JobState.RUNNING, bJob.getState(), "前置成功后继必须自动解锁并启动");
        assertEquals(1, bJob.getAssignedNodeIds().size());
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void cancelPropagatesFailureAlongDependencyChain() {
        SubmissionReport a = scheduler.submit("dep-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport b = scheduler.submit("dep-b", "alice", 5, 1,
                ResourceSet.of(4, 1000, 0), List.of(a.jobId()));
        SubmissionReport c = scheduler.submit("dep-c", "alice", 5, 1,
                ResourceSet.of(4, 1000, 0), List.of(b.jobId()));

        scheduler.cancelJob(a.jobId());
        Job bJob = scheduler.getJob(b.jobId());
        Job cJob = scheduler.getJob(c.jobId());
        log.info("[判定] A 被取消 => B state={} reason={} ; C state={} reason={}",
                bJob.getState(), bJob.getLastReason(), cJob.getState(), cJob.getLastReason());

        assertEquals(JobState.DEPENDENCY_FAILED, bJob.getState(), "前置被取消，直接后继必须落到可区分终态");
        assertTrue(bJob.getLastReason().contains("DEPENDENCY_FAILED") && bJob.getLastReason().contains(a.jobId()),
                "失败原因必须指明是哪个前置、以什么结局结束: " + bJob.getLastReason());
        assertEquals(JobState.DEPENDENCY_FAILED, cJob.getState(), "失败必须沿依赖链级联到间接后继");
        assertEquals(0, totalUsedCpu(), "取消运行中的前置必须完整释放其资源");
        assertTrue(scheduler.isLedgerConsistent());

        // DEPENDENCY_FAILED 是终态：再取消要报状态冲突而不是无声成功
        ScheduleException ex = assertThrows(ScheduleException.class,
                () -> scheduler.cancelJob(b.jobId()));
        log.info("[判定] 对终态作业再取消 => reasonCode={}", ex.getReasonCode());
        assertEquals(ReasonCode.JOB_STATE_CONFLICT, ex.getReasonCode());
    }

    @Test
    void preemptionPropagatesFailureToDependents() {
        // 输入：两个低优先级作业占满两台节点；B 等 A1；高优先级 C 到来只能抢占
        SubmissionReport a1 = scheduler.submit("dep-a1", "alice", 1, 1, ResourceSet.of(8, 1000, 0));
        SubmissionReport a2 = scheduler.submit("dep-a2", "alice", 1, 1, ResourceSet.of(8, 1000, 0));
        assertEquals(JobState.RUNNING, a1.jobState());
        assertEquals(JobState.RUNNING, a2.jobState());
        SubmissionReport b = scheduler.submit("dep-b", "alice", 5, 1,
                ResourceSet.of(4, 1000, 0), List.of(a1.jobId()));
        assertEquals(JobState.WAITING, scheduler.getJob(b.jobId()).getState());

        SubmissionReport c = scheduler.submit("dep-c", "bob", 10, 1, ResourceSet.of(8, 1000, 0));
        Job a1Job = scheduler.getJob(a1.jobId());
        Job bJob = scheduler.getJob(b.jobId());
        log.info("[判定] C(p10) 抢占 => C state={} ; A1 state={} preemptedBy={} ; B state={} reason={}",
                c.jobState(), a1Job.getState(), a1Job.getPreemptedByJobId(),
                bJob.getState(), bJob.getLastReason());

        assertEquals(JobState.RUNNING, c.jobState());
        assertEquals(JobState.PREEMPTED, a1Job.getState(), "被抢占的前置必须进入 PREEMPTED");
        assertEquals(JobState.DEPENDENCY_FAILED, bJob.getState(),
                "前置被抢占（不会成功结束），后继必须落到 DEPENDENCY_FAILED 而不是一直挂着");
        assertTrue(bJob.getLastReason().contains(a1.jobId()));
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void cycleAndUnknownReferenceRejectedWithDistinctCodes() {
        // 引用不存在的作业
        SubmissionReport unknown = scheduler.submit("dep-x", "alice", 5, 1,
                ResourceSet.of(1, 100, 0), List.of("job-999"));
        log.info("[输入] dependsOn=[job-999](不存在) => status={} reason={} msg={}",
                unknown.status(), unknown.reasonCode(), unknown.message());
        assertEquals(SubmissionStatus.REJECTED, unknown.status());
        assertEquals(ReasonCode.DEPENDENCY_NOT_FOUND, unknown.reasonCode());
        assertNull(unknown.jobId());
        assertTrue(scheduler.listJobs().isEmpty(), "被拒绝的提交不得产生作业实例");

        // 自环：本作业将被分配的 id 是 job-1，声明等自己 => 成环
        SubmissionReport selfLoop = scheduler.submit("dep-self", "alice", 5, 1,
                ResourceSet.of(1, 100, 0), List.of("job-1"));
        log.info("[输入] dependsOn=[job-1](自身) => status={} reason={} msg={}",
                selfLoop.status(), selfLoop.reasonCode(), selfLoop.message());
        assertEquals(SubmissionStatus.REJECTED, selfLoop.status());
        assertEquals(ReasonCode.DEPENDENCY_CYCLE, selfLoop.reasonCode());
        assertNotEquals(ReasonCode.DEPENDENCY_NOT_FOUND, selfLoop.reasonCode(),
                "成环与引用不存在必须是可区分的不同原因");
        assertTrue(scheduler.listJobs().isEmpty());

        // 间接环检测（纯函数直接验证）：a->b->c，再让 c->a 即成环
        Map<String, List<String>> graph = Map.of("a", List.of("b"), "b", List.of("c"));
        assertTrue(SchedulerService.wouldCreateCycle(graph, "c", List.of("a")),
                "间接环 c->a->b->c 必须被识别");
        assertFalse(SchedulerService.wouldCreateCycle(Map.of("a", List.of("b")), "c", List.of("a")),
                "普通依赖 c->a->b 不得误判为环");
        assertTrue(SchedulerService.wouldCreateCycle(Map.of(), "x", List.of("x")),
                "自环必须被识别");
        log.info("[判定] 间接环/自环/非环 三种输入的检测结论均符合预期");
    }

    @Test
    void batchUnlockOrderIsDeterministic() {
        // 输入：A 占 1CPU；5 个后继各要 4CPU，总容量 16CPU => 只能起 4 个
        SubmissionReport a = scheduler.submit("dep-a", "alice", 5, 1, ResourceSet.of(1, 100, 0));
        List<String> ids = new ArrayList<>();
        int[] priorities = {5, 9, 9, 5, 7};
        for (int i = 0; i < 5; i++) {
            SubmissionReport r = scheduler.submit("dep-d" + i, "alice", priorities[i], 1,
                    ResourceSet.of(4, 100, 0), List.of(a.jobId()));
            assertEquals(JobState.WAITING, r.jobState());
            ids.add(r.jobId());
        }

        scheduler.completeJob(a.jobId());
        // 判定依据：优先级降序，同优先级先提交先上 => d1(p9), d2(p9), d4(p7), d0(p5) 运行，d3(p5,最迟) 排队
        List<JobState> states = ids.stream().map(id -> scheduler.getJob(id).getState()).toList();
        log.info("[判定] 批量解锁后各后继状态（提交序 d0..d4, 优先级 5,9,9,5,7）: {}", states);
        assertEquals(List.of(JobState.RUNNING, JobState.RUNNING, JobState.RUNNING,
                JobState.QUEUED, JobState.RUNNING), states,
                "解锁顺序必须稳定：优先级降序、同优先级先到先得");
        Job d3 = scheduler.getJob(ids.get(3));
        assertTrue(d3.getLastReason().contains("INSUFFICIENT_CAPACITY"),
                "没排上的后继原因必须是容量不足而非等待依赖: " + d3.getLastReason());
        assertEquals(16, totalUsedCpu(), "4 个后继 x 4CPU，必须恰好占满不超分");
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void concurrentSubmitAndUnlockKeepsLedgerConsistent() throws Exception {
        SubmissionReport a = scheduler.submit("dep-a", "alice", 5, 1, ResourceSet.of(1, 100, 0));
        int threads = 16;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ConcurrentLinkedQueue<SubmissionReport> reports = new ConcurrentLinkedQueue<>();
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            int idx = i;
            Thread t = new Thread(() -> {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                reports.add(scheduler.submit("dep-cc-" + idx, "alice", idx % 3, 1,
                        ResourceSet.of(2, 100, 0), List.of(a.jobId())));
            });
            workers.add(t);
            t.start();
        }
        assertTrue(ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        for (Thread t : workers) {
            t.join(10_000);
        }
        log.info("[输入] {} 线程并发提交不同键的后继（各 2CPU, 前置=A）", threads);

        assertEquals(threads, reports.size());
        long accepted = reports.stream().filter(r -> r.status() == SubmissionStatus.ACCEPTED).count();
        assertEquals(threads, accepted, "不同键的并发提交必须全部接受且各只有一个实例");
        assertTrue(reports.stream().allMatch(r -> r.jobState() == JobState.WAITING),
                "前置未完成时并发提交的后继必须全部 WAITING，不得占用资源");
        assertEquals(1, totalUsedCpu(), "等待期间只有 A 的 1CPU 被占用");

        // 前置完成与解锁同时发生：账本不许乱
        scheduler.completeJob(a.jobId());
        long running = 0;
        long queued = 0;
        for (SubmissionReport r : reports) {
            JobState s = scheduler.getJob(r.jobId()).getState();
            if (s == JobState.RUNNING) {
                running++;
            } else if (s == JobState.QUEUED) {
                queued++;
            }
        }
        log.info("[判定] 并发解锁后 running={} queued={} 已用CPU={} ledgerConsistent={}",
                running, queued, totalUsedCpu(), scheduler.isLedgerConsistent());
        assertEquals(threads, running + queued, "每个后继必须处于 RUNNING 或 QUEUED，不许丢失也不许悬挂");
        assertEquals(8, running, "16CPU / 每作业2CPU => 恰好 8 个运行，不许超分");
        assertEquals(16, totalUsedCpu());
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void idempotentResubmitDoesNotDuplicateDependency() {
        SubmissionReport a = scheduler.submit("dep-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        SubmissionReport first = scheduler.submit("dep-b", "alice", 5, 1,
                ResourceSet.of(4, 1000, 0), List.of(a.jobId()));
        SubmissionReport dup = scheduler.submit("dep-b", "alice", 5, 1,
                ResourceSet.of(4, 1000, 0), List.of(a.jobId()));
        log.info("[判定] 同键同依赖重复提交 => status={} jobId={}（首次 {}）",
                dup.status(), dup.jobId(), first.jobId());
        assertEquals(SubmissionStatus.DUPLICATE, dup.status());
        assertEquals(first.jobId(), dup.jobId());
        assertEquals(1, scheduler.getJob(first.jobId()).getDependsOn().size(),
                "重复提交不得多挂依赖");
        assertEquals(2, scheduler.listJobs().size(), "重复提交不得产生新实例");

        SubmissionReport conflict = scheduler.submit("dep-b", "alice", 5, 1,
                ResourceSet.of(4, 1000, 0), List.of());
        log.info("[判定] 同键不同依赖 => status={} reason={}", conflict.status(), conflict.reasonCode());
        assertEquals(SubmissionStatus.REJECTED, conflict.status());
        assertEquals(ReasonCode.IDEMPOTENCY_KEY_CONFLICT, conflict.reasonCode(),
                "依赖列表属于幂等内容的一部分，不一致必须按冲突拒绝");
        assertEquals(2, scheduler.listJobs().size());
    }

    private long totalUsedCpu() {
        long used = 0;
        for (ClusterNode node : scheduler.listNodes()) {
            used += node.getUsed().get(ResourceType.CPU);
        }
        return used;
    }
}

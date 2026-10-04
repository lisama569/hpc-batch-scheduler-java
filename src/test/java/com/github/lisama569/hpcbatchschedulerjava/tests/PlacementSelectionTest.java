package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.domain.Job;
import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
import com.github.lisama569.hpcbatchschedulerjava.domain.NodeState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.UserQuota;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.ReasonCode;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 作业落点（节点选择）规则：
 * 1) best-fit：小作业落到剩余量最贴近需求的节点，宽裕大节点留给后续大作业；
 * 2) 节点容量异构、且已有部分占用时，按各节点真实剩余量落点；
 * 3) 容量达标但暂时放不下（占用/排空）只排队等待，不被误判成永久不可行；
 *    单机容量硬上限确实不满足才给 NO_FITTING_NODE；
 * 4) 并发提交、提交与完成交错下落点确定、账本守恒、无重复占用。
 */
class PlacementSelectionTest {

    private static final Logger log = LoggerFactory.getLogger(PlacementSelectionTest.class);

    private SchedulerService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SchedulerService();
        // 输入：三台异构节点——小/中/大，配额给足，隔离纯放置问题。
        scheduler.registerNode("n-small", ResourceSet.of(4, 8000, 0));
        scheduler.registerNode("n-mid", ResourceSet.of(8, 16000, 0));
        scheduler.registerNode("n-large", ResourceSet.of(16, 32000, 0));
        scheduler.setQuota(new UserQuota("alice", 100, 100_000, 10_000_000));
        log.info("[setUp] 节点容量 n-small=4C n-mid=8C n-large=16C，alice 配额宽松");
    }

    /** 场景一：小作业必须贴合落点，随后更大的作业仍能立即启动（不再挤占大节点）。 */
    @Test
    void smallJobPacksTightNodeAndLargeJobStillStartsImmediately() {
        // 输入：小作业仅需 4CPU，三台空节点都放得下
        SubmissionReport small = scheduler.submit("small", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        String smallNode = scheduler.getJob(small.jobId()).getAssignedNodeIds().get(0);
        log.info("[输入] small 需要 4CPU => state={} 落点={}（三台均空闲）",
                small.jobState(), smallNode);
        assertEquals(JobState.RUNNING, small.jobState());
        assertEquals("n-small", smallNode,
                "4CPU 需求与 n-small 剩余 4C 完全贴合，必须优先落在最贴近的小节点而非最空闲的大节点");

        // 输入：随后提交需要 16CPU 的大作业，n-small 已占满但 n-large 必须仍可立即承载
        SubmissionReport large = scheduler.submit("large", "alice", 5, 1, ResourceSet.of(16, 1000, 0));
        Job largeJob = scheduler.getJob(large.jobId());
        log.info("[输入] large 需要 16CPU => state={} reason={} 落点={} lastReason={}",
                large.jobState(), large.reasonCode(), largeJob.getAssignedNodeIds(), largeJob.getLastReason());
        assertEquals(JobState.RUNNING, largeJob.getState(),
                "大节点被保留下来，大作业必须立即启动，不得因小作业占了大节点而排队");
        assertEquals(List.of("n-large"), largeJob.getAssignedNodeIds());
        assertTrue(scheduler.isLedgerConsistent());
    }

    /** 场景二：部分占用后按真实剩余量选最贴合节点；多维度资源同样贴合。 */
    @Test
    void partialOccupationUsesRealAvailableResourcesAndBestFitPerDimension() {
        // 输入：先在 n-mid 上放一个 6CPU 作业，n-mid 真实剩余 2CPU
        SubmissionReport occupy = scheduler.submit("occupy-mid", "alice", 5, 1, ResourceSet.of(6, 1000, 0));
        assertEquals(List.of("n-mid"), scheduler.getJob(occupy.jobId()).getAssignedNodeIds(),
                "6CPU 需求在空闲时三台均可放，最贴合的是 n-mid（富余 2）而非 n-large（富余 10）");

        // 输入：再提交 2CPU 作业：n-mid 剩余恰好 2C 最贴合，n-small 剩余 4C，n-large 剩余 16C
        SubmissionReport tiny = scheduler.submit("tiny-2c", "alice", 5, 1, ResourceSet.of(2, 1000, 0));
        String tinyNode = scheduler.getJob(tiny.jobId()).getAssignedNodeIds().get(0);
        log.info("[输入] occupy-mid(6C) 落在 n-mid；tiny 需要 2CPU => 落点={}（n-mid 真实剩余 2C 恰好贴合）",
                tinyNode);
        assertEquals(JobState.RUNNING, tiny.jobState());
        assertEquals("n-mid", tinyNode,
                "必须按节点真实剩余量判定：n-mid 剩 2C 恰好贴合，优于空的 n-small/n-large");

        // 输入：8CPU 大作业——n-mid 已占满、n-small 容量不够，只剩 n-large，应立即启动
        SubmissionReport big = scheduler.submit("big-8c", "alice", 5, 1, ResourceSet.of(8, 1000, 0));
        log.info("[输入] big 需要 8CPU => state={} 落点={}", big.jobState(),
                scheduler.getJob(big.jobId()).getAssignedNodeIds());
        assertEquals(JobState.RUNNING, big.jobState(), "贴合打包保留下来的 n-large 必须能立即承载 8C 作业");

        // 多维度贴合：GPU 需求只能上带 GPU 的节点（本 setUp 三台都无 GPU，另起集群验证）
        SchedulerService gpu = new SchedulerService();
        gpu.registerNode("g-small", ResourceSet.of(64, 64000, 1));
        gpu.registerNode("g-big", ResourceSet.of(64, 64000, 4));
        gpu.setQuota(new UserQuota("alice", 100, 100_000, 10_000_000));
        SubmissionReport gpuJob = gpu.submit("gpu-job", "alice", 5, 1, ResourceSet.of(1, 1000, 1));
        log.info("[输入] 需 1GPU：g-small(1GPU) 与 g-big(4GPU) 均可 => 落点={}（GPU 维度更贴合）",
                gpu.getJob(gpuJob.jobId()).getAssignedNodeIds());
        assertEquals(List.of("g-small"), gpu.getJob(gpuJob.jobId()).getAssignedNodeIds(),
                "多维度资源同样按相对富余率贴合：1GPU 的作业不得占掉 4GPU 大节点");
        assertTrue(scheduler.isLedgerConsistent());
    }

    /** 场景三：容量达标节点只是暂时不可用（排空）=> 临时排队，恢复后立即启动，不是永久不可行。 */
    @Test
    void temporarilyUnavailableCapacityShapedNodesQueueThenResume() {
        // 输入：三台节点全部排空，提交 4CPU 小作业——节点容量都装得下，只是暂时不可调度
        scheduler.setNodeState("n-small", NodeState.DRAINING);
        scheduler.setNodeState("n-mid", NodeState.DRAINING);
        scheduler.setNodeState("n-large", NodeState.DRAINING);
        SubmissionReport blocked = scheduler.submit("blocked", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        Job blockedJob = scheduler.getJob(blocked.jobId());
        log.info("[输入] 三台全部 DRAINING，提交 4CPU 作业 => state={} reason={} basis={}",
                blocked.jobState(), blocked.reasonCode(), blockedJob.getLastReason());
        assertEquals(JobState.QUEUED, blockedJob.getState());
        assertEquals(ReasonCode.INSUFFICIENT_CAPACITY, blocked.reasonCode(),
                "容量装得下、只是节点暂不可调度，必须判为临时容量不足而非永久不可行");
        assertNotNull(blockedJob.getLastReason());

        // 恢复一台最小节点并触发扫描：作业应自动落到恢复的小节点上
        scheduler.setNodeState("n-small", NodeState.HEALTHY);
        int started = scheduler.scheduleTick();
        log.info("[判定] n-small 恢复后 tick 启动数={} blocked state={} 落点={}",
                started, scheduler.getJob(blocked.jobId()).getState(),
                scheduler.getJob(blocked.jobId()).getAssignedNodeIds());
        assertEquals(1, started);
        assertEquals(JobState.RUNNING, scheduler.getJob(blocked.jobId()).getState());
        assertEquals(List.of("n-small"), scheduler.getJob(blocked.jobId()).getAssignedNodeIds());
        assertTrue(scheduler.isLedgerConsistent());
    }

    /** 场景四：单机容量硬上限确实不满足（含“总量够但单机放不下”）=> NO_FITTING_NODE，原因可解释。 */
    @Test
    void hardCapacityCeilingAndAggregateButNotSingleNodeAreExplained() {
        // 输入 A：需要单机 32CPU，最大节点只有 16CPU——任何状态/抢占/等待都改变不了
        SubmissionReport impossible = scheduler.submit("impossible", "alice", 5, 1,
                ResourceSet.of(32, 1000, 0));
        Job impossibleJob = scheduler.getJob(impossible.jobId());
        log.info("[输入] 需要单机 32CPU（最大 16C）=> state={} reason={} basis={}",
                impossible.jobState(), impossible.reasonCode(), impossibleJob.getLastReason());
        assertEquals(JobState.QUEUED, impossibleJob.getState());
        assertEquals(ReasonCode.NO_FITTING_NODE, impossible.reasonCode());
        assertTrue(impossibleJob.getLastReason().contains("NO_FITTING_NODE"),
                "排队原因必须明确指出是单机容量硬上限: " + impossibleJob.getLastReason());

        // 输入 B：总量够但单机放不下——4C 集群上要 2 节点各 6C（总量 8C 需求 12C，单节点形状不满足）
        SchedulerService small = new SchedulerService();
        small.registerNode("s1", ResourceSet.of(4, 8000, 0));
        small.registerNode("s2", ResourceSet.of(4, 8000, 0));
        small.setQuota(new UserQuota("alice", 100, 100_000, 10_000_000));
        SubmissionReport gang = small.submit("gang-6c", "alice", 5, 2, ResourceSet.of(6, 1000, 0));
        Job gangJob = small.getJob(gang.jobId());
        log.info("[输入] 2 台 4C 节点提交 2 节点各 6C 成组作业 => state={} reason={} basis={}",
                gang.jobState(), gang.reasonCode(), gangJob.getLastReason());
        assertEquals(JobState.QUEUED, gangJob.getState());
        assertEquals(ReasonCode.NO_FITTING_NODE, gang.reasonCode(),
                "没有任何单机能装下 6C，等待/抢占都无法产生 6C 单机，必须与临时不足区分");
        small.scheduleTick();
        assertEquals(JobState.QUEUED, small.getJob(gang.jobId()).getState(), "重复调度结论必须一致，不得自行启动");
    }

    /** 场景五：并发提交落点确定（重复运行结论一致）、不超分、不重复占用；提交与完成交错也稳定。 */
    @Test
    void concurrentAndInterleavedSubmissionsGiveStablePackingAndConservedLedger() throws Exception {
        // 5.1 并发：16 个 4CPU 作业同时提交到 4+8+16=28C 集群 => 恰好 7 个运行，每台都被打满
        List<Map<String, Integer>> allocationHistograms = new ArrayList<>();
        for (int round = 0; round < 5; round++) {
            allocationHistograms.add(runConcurrentRound(round));
        }
        log.info("[判定] 5 轮并发提交各节点落点直方图={}", allocationHistograms);
        for (Map<String, Integer> histogram : allocationHistograms) {
            assertEquals(Map.of("n-small", 1, "n-mid", 2, "n-large", 4), histogram,
                    "best-fit 打包 + nodeId 兜底必须让每轮并发结论完全一致：小节点 1 个、中节点 2 个、大节点 4 个");
        }

        // 5.2 交错：提交-完成-再提交，落点与账本始终对得上
        SubmissionReport a = scheduler.submit("interleave-a", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        assertEquals(List.of("n-small"), scheduler.getJob(a.jobId()).getAssignedNodeIds());
        SubmissionReport b = scheduler.submit("interleave-b", "alice", 5, 1, ResourceSet.of(16, 1000, 0));
        assertEquals(JobState.RUNNING, b.jobState(), "大作业在保留下来的大节点上立即运行");
        // n-mid 放一个 8C 后只剩 0C；再来的 4C 作业无落点，应排队且原因可解释
        SubmissionReport c = scheduler.submit("interleave-c", "alice", 5, 1, ResourceSet.of(8, 1000, 0));
        assertEquals(List.of("n-mid"), scheduler.getJob(c.jobId()).getAssignedNodeIds());
        SubmissionReport d = scheduler.submit("interleave-d", "alice", 5, 1, ResourceSet.of(4, 1000, 0));
        assertEquals(JobState.QUEUED, d.jobState());
        assertEquals(ReasonCode.INSUFFICIENT_CAPACITY, d.reasonCode());
        log.info("[输入] 交错提交至全部占满，interleave-d 排队 reason={} basis={}",
                d.reasonCode(), scheduler.getJob(d.jobId()).getLastReason());

        // 小节点作业完成 => 释放出贴合的 4C，排队作业自动落到 n-small
        scheduler.completeJob(a.jobId());
        Job dAfter = scheduler.getJob(d.jobId());
        log.info("[判定] interleave-a 完成释放 n-small 后 interleave-d state={} 落点={}",
                dAfter.getState(), dAfter.getAssignedNodeIds());
        assertEquals(JobState.RUNNING, dAfter.getState(), "资源释放后排队作业必须自动被再次调度，不得一直排不上");
        assertEquals(List.of("n-small"), dAfter.getAssignedNodeIds());

        long usedCpu = scheduler.listNodes().stream()
                .mapToLong(n -> n.getUsed().get(com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType.CPU))
                .sum();
        log.info("[守恒校验] 交错结束后节点已用 CPU={}（期望 28，三台全部打满）账本一致={}",
                usedCpu, scheduler.isLedgerConsistent());
        assertEquals(28, usedCpu, "账目与实际运行必须对得上，不得重复占用或泄漏");
        assertTrue(scheduler.isLedgerConsistent());
    }

    /** 一轮并发提交：16 个 4CPU 作业用栅栏同时起跑，返回 节点 -> 承载作业数 的落点直方图。 */
    private Map<String, Integer> runConcurrentRound(int round) throws Exception {
        SchedulerService svc = new SchedulerService();
        svc.registerNode("n-small", ResourceSet.of(4, 8000, 0));
        svc.registerNode("n-mid", ResourceSet.of(8, 16000, 0));
        svc.registerNode("n-large", ResourceSet.of(16, 32000, 0));
        svc.setQuota(new UserQuota("alice", 100, 100_000, 10_000_000));

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<SubmissionReport>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                start.await();
                return svc.submit("round" + round + "-key-" + idx, "alice", 5, 1,
                        ResourceSet.of(4, 1000, 0));
            }));
        }
        start.countDown();
        List<SubmissionReport> reports = new ArrayList<>();
        for (Future<SubmissionReport> f : futures) {
            reports.add(f.get(10, TimeUnit.SECONDS));
        }
        pool.shutdown();

        long running = reports.stream().filter(r -> r.jobState() == JobState.RUNNING).count();
        long queued = reports.stream().filter(r -> r.jobState() == JobState.QUEUED).count();
        Map<String, Integer> histogram = new ConcurrentHashMap<>();
        for (SubmissionReport r : reports) {
            Job job = svc.getJob(r.jobId());
            if (job.getState() == JobState.RUNNING) {
                for (String node : job.getAssignedNodeIds()) {
                    histogram.merge(node, 1, Integer::sum);
                }
            }
        }
        log.info("[输入|round{}] {} 个 4CPU 作业并发提交（容量 28C）", round, threads);
        log.info("[判定|round{}] running={} queued={} 落点直方图={} 账本一致={}",
                round, running, queued, histogram, svc.isLedgerConsistent());
        assertEquals(7, running, "28C / 每作业 4C => 恰好 7 个运行，不得超分");
        assertEquals(threads - 7, queued);
        assertTrue(svc.isLedgerConsistent());
        return histogram;
    }
}

package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.domain.Job;
import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType;
import com.github.lisama569.hpcbatchschedulerjava.domain.UserQuota;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.ReasonCode;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionReport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 作业落点（best-fit 节点选择）：
 * 1) 不同容量节点下，小作业落到剩余资源最贴近需求的节点，随后的大作业仍能立即启动；
 * 2) 节点被部分占用后，按各节点真实剩余量选择落点；
 * 3) 确实放不下时排队原因可区分（临时不足 vs 单机硬上限），
 *    且“总需求超单机容量但每节点需求可装下”不被误判为不可行；
 * 4) 并发提交与提交/完成交错时，落点确定、账本不超分，同批作业重复调度结论一致。
 */
class BestFitPlacementTest {

    private static final Logger log = LoggerFactory.getLogger(BestFitPlacementTest.class);

    private SchedulerService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SchedulerService();
        scheduler.setQuota(new UserQuota("u", 1000, 1_000_000, 1_000_000_000));
    }

    @Test
    void smallJobTakesTightestNodeAndBigJobStillStartsImmediately() {
        // 输入：三种机型，小 4CPU / 中 8CPU / 大 32CPU（内存置 0，让判定依据只看 CPU 维度）
        scheduler.registerNode("small", ResourceSet.of(4, 0, 0));
        scheduler.registerNode("mid", ResourceSet.of(8, 0, 0));
        scheduler.registerNode("large", ResourceSet.of(32, 0, 0));
        log.info("[输入] 节点容量: small=4C mid=8C large=32C");

        // 小作业 2CPU：应落到剩余量最贴近的 small（富余 2），而不是最大的 large（富余 30）
        SubmissionReport small = scheduler.submit("small-job", "u", 5, 1, ResourceSet.of(2, 0, 0));
        Job smallJob = scheduler.getJob(small.jobId());
        log.info("[判定] 小作业 2C => state={} nodes={}（期望 [small]：富余分 2 < mid 6 < large 30）",
                smallJob.getState(), smallJob.getAssignedNodeIds());
        assertEquals(JobState.RUNNING, smallJob.getState());
        assertEquals(List.of("small"), smallJob.getAssignedNodeIds(),
                "小作业必须落到最贴合的节点，把宽裕节点留给大作业");

        // 随后的大作业 16CPU：只有 large 装得下，必须能立即启动
        SubmissionReport big = scheduler.submit("big-job", "u", 5, 1, ResourceSet.of(16, 0, 0));
        Job bigJob = scheduler.getJob(big.jobId());
        log.info("[判定] 大作业 16C => state={} nodes={}（期望 [large] 立即启动）",
                bigJob.getState(), bigJob.getAssignedNodeIds());
        assertEquals(JobState.RUNNING, bigJob.getState(),
                "小作业已落在 small 上，大作业必须能立即在 large 上启动");
        assertEquals(List.of("large"), bigJob.getAssignedNodeIds());
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void placementUsesRealRemainingAfterPartialOccupancy() {
        // 输入：n1/n2 各 8CPU，n3 16CPU
        scheduler.registerNode("n1", ResourceSet.of(8, 0, 0));
        scheduler.registerNode("n2", ResourceSet.of(8, 0, 0));
        scheduler.registerNode("n3", ResourceSet.of(16, 0, 0));

        // 先占 n1 6CPU（同分时按节点 id，落 n1），再占 2CPU（n1 剩余 2 最贴合，填满 n1）
        Job f1 = scheduler.getJob(scheduler.submit("f1", "u", 5, 1, ResourceSet.of(6, 0, 0)).jobId());
        Job f2 = scheduler.getJob(scheduler.submit("f2", "u", 5, 1, ResourceSet.of(2, 0, 0)).jobId());
        log.info("[输入] 填充后: n1 剩余={} n2 剩余={} n3 剩余={}（f1={} f2={}）",
                scheduler.getNode("n1").available(), scheduler.getNode("n2").available(),
                scheduler.getNode("n3").available(), f1.getAssignedNodeIds(), f2.getAssignedNodeIds());
        assertEquals(List.of("n1"), f1.getAssignedNodeIds());
        assertEquals(List.of("n1"), f2.getAssignedNodeIds());

        // 4CPU 作业：n2 剩余 8（富余 4）比 n3 剩余 16（富余 12）更贴合 => 落 n2
        Job j4 = scheduler.getJob(scheduler.submit("j4", "u", 5, 1, ResourceSet.of(4, 0, 0)).jobId());
        log.info("[判定] 4C 作业 => nodes={}（期望 [n2]：按真实剩余量 best-fit，而非最空闲的 n3）",
                j4.getAssignedNodeIds());
        assertEquals(List.of("n2"), j4.getAssignedNodeIds());

        // 8CPU 作业：n2 剩 4 装不下，只有 n3 能放 => 落 n3（部分占用不影响按剩余量判定）
        Job j8 = scheduler.getJob(scheduler.submit("j8", "u", 5, 1, ResourceSet.of(8, 0, 0)).jobId());
        log.info("[判定] 8C 作业 => nodes={}（期望 [n3]：n2 剩余不足，n3 装得下就必须启动）",
                j8.getAssignedNodeIds());
        assertEquals(JobState.RUNNING, j8.getState());
        assertEquals(List.of("n3"), j8.getAssignedNodeIds());
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void queueReasonsAreDistinguishableWhenNothingFits() {
        scheduler.registerNode("n1", ResourceSet.of(8, 0, 0));
        scheduler.registerNode("n2", ResourceSet.of(8, 0, 0));

        // 把两个节点占满
        scheduler.submit("fill-1", "u", 5, 1, ResourceSet.of(8, 0, 0));
        scheduler.submit("fill-2", "u", 5, 1, ResourceSet.of(8, 0, 0));

        // 临时放不下：4CPU 作业排队，原因必须是 INSUFFICIENT_CAPACITY 且带逐节点缺额
        SubmissionReport queued = scheduler.submit("queued-4c", "u", 5, 1, ResourceSet.of(4, 0, 0));
        Job queuedJob = scheduler.getJob(queued.jobId());
        log.info("[判定] 集群占满后提交 4C => state={} reason={} lastReason={}",
                queuedJob.getState(), queued.reasonCode(), queuedJob.getLastReason());
        assertEquals(JobState.QUEUED, queuedJob.getState());
        assertEquals(ReasonCode.INSUFFICIENT_CAPACITY, queued.reasonCode());
        assertTrue(queuedJob.getLastReason().contains("INSUFFICIENT_CAPACITY"),
                "排队原因必须可解释并含原因码");

        // 单机硬上限：16CPU 超过任何单机容量 => NO_FITTING_NODE，与临时不足可区分
        SubmissionReport tooBig = scheduler.submit("too-big", "u", 5, 1, ResourceSet.of(16, 0, 0));
        log.info("[判定] 单机装不下的 16C => reason={}（期望 NO_FITTING_NODE）", tooBig.reasonCode());
        assertEquals(ReasonCode.NO_FITTING_NODE, tooBig.reasonCode());

        // 释放一个节点后，排队的 4CPU 作业必须能自动启动（不是“永远排不上”）
        scheduler.completeJob("job-1");
        scheduler.scheduleTick();
        log.info("[判定] 释放 n1 后 queued-4c state={} nodes={}",
                queuedJob.getState(), queuedJob.getAssignedNodeIds());
        assertEquals(JobState.RUNNING, queuedJob.getState());
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void gangTotalExceedingSingleNodeCapacityIsNotMisjudged() {
        // 输入：两台 4CPU 节点；作业要 2 节点 x 3CPU，总需求 6CPU 超过任一单机容量
        scheduler.registerNode("n1", ResourceSet.of(4, 0, 0));
        scheduler.registerNode("n2", ResourceSet.of(4, 0, 0));
        SubmissionReport gang = scheduler.submit("gang", "u", 5, 2, ResourceSet.of(3, 0, 0));
        Job gangJob = scheduler.getJob(gang.jobId());
        log.info("[判定] 2节点x3C（总量6C>单机4C）=> state={} reason={} nodes={}",
                gangJob.getState(), gang.reasonCode(), gangJob.getAssignedNodeIds());
        assertEquals(JobState.RUNNING, gangJob.getState(),
                "每节点需求装得下就必须调度，不得按总量对单机容量误判为不可行");
        assertNotEquals(ReasonCode.NO_FITTING_NODE, gang.reasonCode());
        assertEquals(2, gangJob.getAssignedNodeIds().size());
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void concurrentAndInterleavedSchedulingIsDeterministicAndConserved() throws Exception {
        // 输入：8+8+16=32CPU；16 个并发作业各 2CPU，恰好全部装下
        scheduler.registerNode("n1", ResourceSet.of(8, 0, 0));
        scheduler.registerNode("n2", ResourceSet.of(8, 0, 0));
        scheduler.registerNode("n3", ResourceSet.of(16, 0, 0));

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<SubmissionReport>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                start.await();
                return scheduler.submit("c-" + idx, "u", 5, 1, ResourceSet.of(2, 0, 0));
            }));
        }
        start.countDown();
        for (Future<SubmissionReport> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        long running = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING).count();
        log.info("[判定] 16 并发 2C 作业 => running={}（期望 16，恰好占满 32C）账本一致={}",
                running, scheduler.isLedgerConsistent());
        assertEquals(threads, running, "总需求恰好等于总容量，必须全部运行且不超分");
        assertNodeAccountsMatchJobs();

        // 记录并发批次的落点结论
        Map<String, List<String>> firstRound = placementsByShape();

        // 提交/完成交错：完成一半作业，同时再提交一批同形作业
        List<Job> runningJobs = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING).toList();
        for (int i = 0; i < runningJobs.size(); i += 2) {
            scheduler.completeJob(runningJobs.get(i).getJobId());
            scheduler.submit("r-" + i, "u", 5, 1, ResourceSet.of(2, 0, 0));
        }
        scheduler.scheduleTick();
        log.info("[判定] 交错完成/再提交后 running={} 账本一致={}",
                scheduler.listJobs().stream().filter(j -> j.getState() == JobState.RUNNING).count(),
                scheduler.isLedgerConsistent());
        assertNodeAccountsMatchJobs();

        // 全部完成后，按相同形状整批重交：落点结论必须与第一轮完全一致
        for (Job j : scheduler.listJobs()) {
            if (j.getState() == JobState.RUNNING) {
                scheduler.completeJob(j.getJobId());
            }
        }
        for (int i = 0; i < threads; i++) {
            scheduler.submit("again-" + i, "u", 5, 1, ResourceSet.of(2, 0, 0));
        }
        Map<String, List<String>> secondRound = placementsByShape();
        List<String> firstPlacements = sortedPlacements(firstRound);
        List<String> secondPlacements = sortedPlacements(secondRound);
        log.info("[判定] 同批作业重复调度: 第一轮落点={} 第二轮落点={} 一致={}",
                firstPlacements, secondPlacements, firstPlacements.equals(secondPlacements));
        assertEquals(firstPlacements, secondPlacements, "同一批作业重复调度，落点结论必须一致");
        assertTrue(scheduler.isLedgerConsistent());
    }

    /** 落点序列（排序后）：两轮作业 jobId 不同，只能按“落在哪些节点”的序列比较。 */
    private List<String> sortedPlacements(Map<String, List<String>> placements) {
        return placements.values().stream()
                .map(nodes -> String.join("+", nodes))
                .sorted()
                .toList();
    }

    /** 按作业形状汇总落点（本测试作业同形，即按提交顺序的落点序列）。 */
    private Map<String, List<String>> placementsByShape() {
        Map<String, List<String>> placements = new LinkedHashMap<>();
        for (Job j : scheduler.listJobs()) {
            if (j.getState() == JobState.RUNNING) {
                placements.put(j.getJobId(), List.copyOf(j.getAssignedNodeIds()));
            }
        }
        return placements;
    }

    /** 每台节点账本占用必须等于“分配到该节点的运行作业”汇总，且不超过容量。 */
    private void assertNodeAccountsMatchJobs() {
        for (var node : scheduler.listNodes()) {
            long expectedCpu = 0;
            for (Job j : scheduler.listJobs()) {
                if (j.getState() == JobState.RUNNING
                        && j.getAssignedNodeIds().contains(node.getNodeId())) {
                    expectedCpu += j.getPerNodeResource().get(ResourceType.CPU);
                }
            }
            long usedCpu = node.getUsed().get(ResourceType.CPU);
            long capacityCpu = node.getCapacity().get(ResourceType.CPU);
            log.info("[账目核对] 节点={} 账本已用={} 运行作业汇总={} 容量={}",
                    node.getNodeId(), usedCpu, expectedCpu, capacityCpu);
            assertEquals(expectedCpu, usedCpu, node.getNodeId() + " 账本与运行作业不一致");
            assertTrue(usedCpu <= capacityCpu, node.getNodeId() + " 占用超过容量");
        }
    }
}

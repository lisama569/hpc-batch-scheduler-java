package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.domain.Job;
import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType;
import com.github.lisama569.hpcbatchschedulerjava.domain.UserQuota;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 跨节点、多作业混合生命周期（提交/完成/取消/抢占/排空恢复）后，
 * 从“运行作业视角”汇总的占用必须与节点账本逐类一致，且不超过总容量。
 */
class ResourceConservationTest {

    private static final Logger log = LoggerFactory.getLogger(ResourceConservationTest.class);

    @Test
    void mixedLifecycleKeepsLedgerConservedAcrossAllDimensions() {
        SchedulerService scheduler = new SchedulerService();
        scheduler.registerNode("n1", ResourceSet.of(8, 16000, 4));
        scheduler.registerNode("n2", ResourceSet.of(8, 16000, 4));
        scheduler.setQuota(new UserQuota("u", 1000, 10_000, 10_000_000));

        List<String> running = new ArrayList<>();

        // 一批低优先级作业填满集群（每节点两个 4CPU/1GPU）
        for (int i = 0; i < 4; i++) {
            var r = scheduler.submit("fill-" + i, "u", 1, 1, ResourceSet.of(4, 2000, 1));
            running.add(r.jobId());
        }
        assertConserved(scheduler, "填充 4 个低优先级作业后");

        // 一批高优先级成组作业：需要 2 节点各 6CPU/2GPU，必然触发抢占
        var gangHigh = scheduler.submit("gang-high", "u", 9, 2, ResourceSet.of(6, 4000, 2));
        log.info("[输入] gang-high 需 2 节点 x 6C4G2GPU => state={} nodes={}",
                gangHigh.jobState(), scheduler.getJob(gangHigh.jobId()).getAssignedNodeIds());
        assertEquals(JobState.RUNNING, gangHigh.jobState(), "高优先级成组作业应通过抢占启动");
        assertConserved(scheduler, "成组抢占后");

        long preempted = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.PREEMPTED).count();
        log.info("[判定] 被抢占作业数={}（每个被抢占作业占用须完整回收）", preempted);
        assertTrue(preempted >= 2, "跨两个节点的成组需求应至少抢占两个作业");

        // 取消/完成若干作业并反复 tick
        scheduler.completeJob(gangHigh.jobId());
        scheduler.scheduleTick();
        assertConserved(scheduler, "高优先级作业完成并 tick 后");

        // 反复排空/恢复节点并 tick，账本仍需守恒
        scheduler.setNodeState("n1", com.github.lisama569.hpcbatchschedulerjava.domain.NodeState.DRAINING);
        scheduler.scheduleTick();
        scheduler.setNodeState("n1", com.github.lisama569.hpcbatchschedulerjava.domain.NodeState.HEALTHY);
        scheduler.scheduleTick();
        assertConserved(scheduler, "节点排空-恢复并 tick 后");

        // 清空所有仍在运行的作业
        for (Job job : scheduler.listJobs()) {
            if (job.getState() == JobState.RUNNING) {
                scheduler.completeJob(job.getJobId());
            }
        }
        scheduler.scheduleTick();
        long remainingCpu = sumRunning(scheduler, ResourceType.CPU);
        long remainingGpu = sumRunning(scheduler, ResourceType.GPU);
        log.info("[守恒校验] 全部完成后运行占用 CPU={} GPU={} 账本一致={}",
                remainingCpu, remainingGpu, scheduler.isLedgerConsistent());
        assertEquals(0, remainingCpu);
        assertEquals(0, remainingGpu);
        assertTrue(scheduler.isLedgerConsistent());
    }

    private long sumRunning(SchedulerService scheduler, ResourceType type) {
        return scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING)
                .mapToLong(j -> j.getPerNodeResource().get(type) * j.getNodeCount())
                .sum();
    }

    private void assertConserved(SchedulerService scheduler, String phase) {
        long cpuFromJobs = sumRunning(scheduler, ResourceType.CPU);
        long memFromJobs = sumRunning(scheduler, ResourceType.MEMORY_MB);
        long gpuFromJobs = sumRunning(scheduler, ResourceType.GPU);
        boolean consistent = scheduler.isLedgerConsistent();
        log.info("[守恒校验|{}] 运行视角占用 CPU={} MEM={} GPU={} ledgerConsistent={}",
                phase, cpuFromJobs, memFromJobs, gpuFromJobs, consistent);
        assertTrue(consistent, phase + ": 节点容量与实际占用不一致");
        // 被抢占/已完成作业不得残留占用：节点账本必须与运行作业视角完全相等
        var nodes = scheduler.listNodes();
        long cpuFromNodes = nodes.stream().mapToLong(n -> n.getUsed().get(ResourceType.CPU)).sum();
        long memFromNodes = nodes.stream().mapToLong(n -> n.getUsed().get(ResourceType.MEMORY_MB)).sum();
        long gpuFromNodes = nodes.stream().mapToLong(n -> n.getUsed().get(ResourceType.GPU)).sum();
        assertEquals(cpuFromJobs, cpuFromNodes, phase + " CPU 账本与运行视角不一致（可能泄漏）");
        assertEquals(memFromJobs, memFromNodes, phase + " MEM 账本与运行视角不一致（可能泄漏）");
        assertEquals(gpuFromJobs, gpuFromNodes, phase + " GPU 账本与运行视角不一致（可能泄漏）");
    }
}

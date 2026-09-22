package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType;
import com.github.lisama569.hpcbatchschedulerjava.domain.UserQuota;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionReport;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionStatus;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发提交：
 * 1) 相同幂等键的高并发提交只产生一个运行实例；
 * 2) 不同作业的并发提交在账本上总量守恒、无超分。
 */
class ConcurrentSubmitTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentSubmitTest.class);

    @Test
    void concurrentDuplicateKeysProduceSingleInstance() throws Exception {
        SchedulerService scheduler = new SchedulerService();
        scheduler.registerNode("n1", ResourceSet.of(64, 64000, 8));
        scheduler.setQuota(new UserQuota("u", 1000, 100_000, 10_000_000));

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<SubmissionReport>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return scheduler.submit("same-key", "u", 5, 1, ResourceSet.of(4, 1000, 1));
            }));
        }
        start.countDown();

        List<SubmissionReport> reports = new ArrayList<>();
        for (Future<SubmissionReport> f : futures) {
            reports.add(f.get(10, TimeUnit.SECONDS));
        }
        pool.shutdown();

        long accepted = reports.stream().filter(r -> r.status() == SubmissionStatus.ACCEPTED).count();
        long duplicate = reports.stream().filter(r -> r.status() == SubmissionStatus.DUPLICATE).count();
        String onlyJobId = reports.get(0).jobId();
        boolean sameJob = reports.stream().allMatch(r -> r.jobId().equals(onlyJobId));

        log.info("[输入] {} 个线程并发提交相同幂等键 same-key", threads);
        log.info("[判定] ACCEPTED={} DUPLICATE={} 全部返回同一 jobId={} 一致={}",
                accepted, duplicate, onlyJobId, sameJob);
        log.info("[守恒校验] 作业总数={} 账本一致={}",
                scheduler.listJobs().size(), scheduler.isLedgerConsistent());

        assertEquals(1, accepted, "只能有一个请求真正创建并占用资源");
        assertEquals(threads - 1, duplicate, "其余请求必须识别为重复");
        assertTrue(sameJob);
        assertEquals(1, scheduler.listJobs().size());
        assertTrue(scheduler.isLedgerConsistent());
    }

    @Test
    void concurrentDistinctJobsNeverOverAllocate() throws Exception {
        SchedulerService scheduler = new SchedulerService();
        // 总容量 8 CPU
        scheduler.registerNode("n1", ResourceSet.of(4, 8000, 0));
        scheduler.registerNode("n2", ResourceSet.of(4, 8000, 0));
        scheduler.setQuota(new UserQuota("u", 1000, 100_000, 10_000_000));

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<SubmissionReport>> futures = new ArrayList<>();
        // 每个作业要 2 CPU，最多只有 4 个能运行
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                start.await();
                return scheduler.submit("key-" + idx, "u", 5, 1, ResourceSet.of(2, 500, 0));
            }));
        }
        start.countDown();
        for (Future<SubmissionReport> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }
        pool.shutdown();

        long running = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING).count();
        long queued = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.QUEUED).count();
        long runningCpu = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING)
                .mapToLong(j -> j.getPerNodeResource().get(ResourceType.CPU) * j.getNodeCount())
                .sum();

        log.info("[输入] {} 个不同作业（各 2CPU）并发提交到总容量 8CPU 的集群", threads);
        log.info("[判定] running={} queued={} 运行占用CPU={}（必须 <= 8）账本一致={}",
                running, queued, runningCpu, scheduler.isLedgerConsistent());

        assertEquals(4, running, "总容量 8CPU / 每作业 2CPU => 恰好 4 个运行，其余排队");
        assertEquals(threads - 4, queued);
        assertEquals(8, runningCpu, "实际占用不得超过节点容量");
        assertTrue(scheduler.isLedgerConsistent());
    }
}

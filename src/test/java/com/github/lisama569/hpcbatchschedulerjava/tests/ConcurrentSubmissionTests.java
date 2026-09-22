package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.model.TaskSpec;
import com.github.lisama569.hpcbatchschedulerjava.quota.QuotaManager;
import com.github.lisama569.hpcbatchschedulerjava.service.ClusterService;
import com.github.lisama569.hpcbatchschedulerjava.service.DuplicateSubmissionException;
import com.github.lisama569.hpcbatchschedulerjava.service.JobService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** 并发提交：重复幂等键只产生一个实例，账本不变量始终成立。 */
class ConcurrentSubmissionTests {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentSubmissionTests.class);

    @Test
    void concurrentIdenticalSubmissionsCreateSingleJob() throws Exception {
        ResourceLedger ledger = new ResourceLedger();
        ledger.addNode("n1", Resources.of(64, 131072, 16));
        ledger.addNode("n2", Resources.of(64, 131072, 16));
        QuotaManager quota = new QuotaManager();
        JobService jobs = new JobService(ledger, quota);
        ClusterService cluster = new ClusterService(ledger, jobs, jobs.schedulingLock());

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicated = new AtomicInteger();
        ConcurrentHashMap<String, Boolean> acceptedIds = new ConcurrentHashMap<>();

        log.info("[输入] {} 个线程同时提交相同幂等键 concurrent-key，任务 cpu=2", threads);
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    var job = jobs.submit("concurrent-key", "u1", 0,
                            List.of(new TaskSpec("t0", Resources.of(2, 1024, 0))));
                    accepted.incrementAndGet();
                    acceptedIds.put(job.id(), Boolean.TRUE);
                } catch (DuplicateSubmissionException e) {
                    duplicated.incrementAndGet();
                    acceptedIds.put(e.existingJob().id(), Boolean.TRUE);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "并发提交应在超时前全部返回");
        pool.shutdown();

        log.info("[判定] 接受={} 重复拒绝={} 观察到的作业 id={} 作业总数={} 账本违例={}",
                accepted.get(), duplicated.get(), acceptedIds.keySet(), jobs.listJobs().size(),
                cluster.verifyInvariants());
        assertEquals(1, accepted.get(), "恰好一个请求成功创建作业");
        assertEquals(threads - 1, duplicated.get(), "其余请求必须以重复提交被拒绝");
        assertEquals(1, jobs.listJobs().size(), "只能存在一个作业实例");
        assertEquals(1, acceptedIds.size(), "所有响应指向同一个作业 id");
        assertEquals(2, ledger.getNode("n1").used()
                .get(com.github.lisama569.hpcbatchschedulerjava.model.ResourceType.CPU),
                "资源只被占用一次，无重复分配");
        assertTrue(cluster.verifyInvariants().isEmpty(),
                "并发下容量守恒: " + cluster.verifyInvariants());
    }
}

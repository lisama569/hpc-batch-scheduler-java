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

/** 节点排空不接收新作业，恢复后重新参与调度。 */
class NodeDrainTests {

    private static final Logger log = LoggerFactory.getLogger(NodeDrainTests.class);

    @Test
    void drainingNodeRejectsNewPlacementsAndRecovers() {
        ResourceLedger ledger = new ResourceLedger();
        ledger.addNode("n1", Resources.of(4, 8192, 1));
        ledger.addNode("n2", Resources.of(4, 8192, 1));
        QuotaManager quota = new QuotaManager();
        JobService jobs = new JobService(ledger, quota);
        ClusterService cluster = new ClusterService(ledger, jobs, jobs.schedulingLock());

        // 成组作业需要两个节点各放一个任务
        Job gang1 = jobs.submit("d1", "u1", 0,
                List.of(new TaskSpec("t0", Resources.of(3, 1024, 1)),
                        new TaskSpec("t1", Resources.of(3, 1024, 1))));
        log.info("[输入] 成组作业 {} 落点={}", gang1.id(), gang1.placement());
        assertEquals(JobState.RUNNING, gang1.state());

        // 排空 n1 并驱逐：gang1 只剩 n2 无法成组，因此保持 QUEUED，n1 资源完整回收
        List<String> evicted = cluster.drainNode("n1", true);
        Job gang1After = jobs.findJob(gang1.id()).orElseThrow();
        log.info("[判定] 驱逐列表={}，n1 占用={}，被驱逐作业状态={}",
                evicted, ledger.getNode("n1").used().describe(), gang1After.state());
        assertEquals(List.of(gang1.id()), evicted);
        assertEquals(JobState.QUEUED, gang1After.state());
        assertEquals(0, ledger.getNode("n1").used()
                .get(com.github.lisama569.hpcbatchschedulerjava.model.ResourceType.CPU),
                "排空驱逐必须完整回收资源");

        // 排空期间：新作业不得落到 n1（只能落 n2）
        Job small = jobs.submit("d2", "u2", 0,
                List.of(new TaskSpec("t0", Resources.of(1, 1024, 0))));
        log.info("[判定] 排空期间新作业落点={}（期望 n2，绝不落 n1）", small.placement());
        assertEquals("n2", small.placement().get(0));

        // 高优先级成组作业：即使抢占 n2 上的小作业，n1 排空也无法成组 -> 排队且不白抢
        Job gang2 = jobs.submit("d3", "u3", 5,
                List.of(new TaskSpec("t0", Resources.of(3, 1024, 1)),
                        new TaskSpec("t1", Resources.of(3, 1024, 1))));
        log.info("[判定] n1 排空时高优先级成组作业状态={}，n2 小作业抢占次数=0（不白抢）", gang2.state());
        assertEquals(JobState.QUEUED, gang2.state());
        assertEquals(0, small.preemptCount());

        // 恢复 n1：高优先级 gang2 成组启动（n1 + n2 各一个任务）
        cluster.recoverNode("n1");
        log.info("[判定] n1 恢复后: gang2 状态={} 落点={}; gang1 状态={}",
                jobs.findJob(gang2.id()).orElseThrow().state(),
                jobs.findJob(gang2.id()).orElseThrow().placement(),
                jobs.findJob(gang1.id()).orElseThrow().state());
        assertEquals(JobState.RUNNING, jobs.findJob(gang2.id()).orElseThrow().state());
        assertTrue(cluster.verifyInvariants().isEmpty(),
                "恢复调度后容量守恒: " + cluster.verifyInvariants());
    }
}

package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Job;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.model.TaskSpec;
import com.github.lisama569.hpcbatchschedulerjava.quota.QuotaManager;
import com.github.lisama569.hpcbatchschedulerjava.service.ClusterService;
import com.github.lisama569.hpcbatchschedulerjava.service.DuplicateSubmissionException;
import com.github.lisama569.hpcbatchschedulerjava.service.InvalidRequestException;
import com.github.lisama569.hpcbatchschedulerjava.service.JobService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 非法请求与重复提交必须可区分，重复提交不产生多个实例/不重复占资源。 */
class JobSubmissionTests {

    private static final Logger log = LoggerFactory.getLogger(JobSubmissionTests.class);

    private record Harness(ResourceLedger ledger, JobService jobs, ClusterService cluster) {
    }

    private Harness harness() {
        ResourceLedger ledger = new ResourceLedger();
        ledger.addNode("n1", Resources.of(8, 16384, 2));
        ledger.addNode("n2", Resources.of(8, 16384, 2));
        QuotaManager quota = new QuotaManager();
        JobService jobs = new JobService(ledger, quota);
        ClusterService cluster = new ClusterService(ledger, jobs, jobs.schedulingLock());
        return new Harness(ledger, jobs, cluster);
    }

    private List<TaskSpec> oneTask(long cpu) {
        return List.of(new TaskSpec("t0", Resources.of(cpu, 1024, 0)));
    }

    @Test
    void rejectsInvalidRequests() {
        Harness h = harness();
        log.info("[输入] 三种非法请求: 空幂等键 / 空 owner / 空任务列表");
        assertThrows(InvalidRequestException.class,
                () -> h.jobs().submit(" ", "u1", 0, oneTask(1)), "空幂等键必须拒绝");
        assertThrows(InvalidRequestException.class,
                () -> h.jobs().submit("k1", " ", 0, oneTask(1)), "空 owner 必须拒绝");
        assertThrows(InvalidRequestException.class,
                () -> h.jobs().submit("k1", "u1", 0, List.of()), "空任务列表必须拒绝");
        log.info("[判定] 三类非法请求均抛 InvalidRequestException（与重复提交的 DuplicateSubmissionException 可区分）");
        assertTrue(h.cluster().verifyInvariants().isEmpty());
        assertTrue(h.jobs().listJobs().isEmpty(), "非法请求不得创建作业");
    }

    @Test
    void duplicateSubmissionReturnsSameJobAndHoldsResourcesOnce() {
        Harness h = harness();
        Job first = h.jobs().submit("idem-1", "u1", 5, oneTask(4));
        log.info("[输入] 首次提交 idem-1 cpu=4 => job={} 状态={}", first.id(), first.state());
        DuplicateSubmissionException ex = assertThrows(DuplicateSubmissionException.class,
                () -> h.jobs().submit("idem-1", "u1", 5, oneTask(4)));
        log.info("[判定] 重复提交被拒绝，返回既有作业 {} 状态={}（异常类型不同于非法请求）",
                ex.existingJob().id(), ex.existingJob().state());
        assertEquals(first.id(), ex.existingJob().id());
        assertEquals(1, h.jobs().listJobs().size(), "重复提交不得产生第二个实例");
        assertEquals(4, h.ledger().getNode("n1").used()
                .get(com.github.lisama569.hpcbatchschedulerjava.model.ResourceType.CPU),
                "资源只被占用一次");
        assertTrue(h.cluster().verifyInvariants().isEmpty(), "账本守恒: " + h.cluster().verifyInvariants());
    }
}

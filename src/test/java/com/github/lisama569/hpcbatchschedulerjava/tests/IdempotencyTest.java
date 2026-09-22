package com.github.lisama569.hpcbatchschedulerjava.tests;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * 幂等与非法请求：
 * 相同幂等键的重复提交不产生第二个实例、不重复占用资源；
 * 同键不同内容可区分拒绝；非法参数以 INVALID_REQUEST 异常拒绝。
 */
class IdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyTest.class);

    private SchedulerService scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SchedulerService();
        scheduler.registerNode("n1", ResourceSet.of(4, 8000, 1));
        scheduler.setQuota(new UserQuota("alice", 10, 100, 100_000));
        log.info("[setUp] n1=4C8G1GPU；alice 配额宽松");
    }

    @Test
    void duplicateSubmissionReusesInstanceAndDoesNotDoubleAllocate() {
        SubmissionReport first = scheduler.submit("key-1", "alice", 5, 1, ResourceSet.of(2, 1000, 0));
        log.info("[输入1] key-1 2C1000MB => jobId={} state={}", first.jobId(), first.jobState());

        SubmissionReport second = scheduler.submit("key-1", "alice", 5, 1, ResourceSet.of(2, 1000, 0));
        log.info("[输入2] 相同 key 相同内容 => status={} reason={} jobId={}",
                second.status(), second.reasonCode(), second.jobId());

        assertSame(SubmissionStatus.DUPLICATE, second.status());
        assertEquals(ReasonCode.DUPLICATE_SUBMISSION, second.reasonCode());
        assertEquals(first.jobId(), second.jobId(), "重复提交必须复用同一作业实例");
        assertEquals(1, scheduler.listJobs().size(), "不得产生第二个作业实例");

        long runningCpu = scheduler.listJobs().stream()
                .filter(j -> j.getState() == JobState.RUNNING)
                .mapToLong(j -> j.getPerNodeResource().get(ResourceType.CPU) * j.getNodeCount())
                .sum();
        log.info("[守恒校验] 运行中 CPU 累计={}（期望 2，重复提交不得翻倍），账本一致={}",
                runningCpu, scheduler.isLedgerConsistent());
        assertEquals(2, runningCpu);
        assertEquals(true, scheduler.isLedgerConsistent());
    }

    @Test
    void sameKeyDifferentPayloadIsRejectedWithConflictReason() {
        scheduler.submit("key-2", "alice", 5, 1, ResourceSet.of(2, 1000, 0));
        SubmissionReport conflict = scheduler.submit("key-2", "alice", 9, 1, ResourceSet.of(3, 1000, 0));
        log.info("[输入] 同 key 不同内容（优先级/CPU 变化）=> status={} reason={} msg={}",
                conflict.status(), conflict.reasonCode(), conflict.message());
        assertSame(SubmissionStatus.REJECTED, conflict.status());
        assertEquals(ReasonCode.IDEMPOTENCY_KEY_CONFLICT, conflict.reasonCode(),
                "同键内容冲突必须与普通重复区分开");
        assertEquals(1, scheduler.listJobs().size());
    }

    @Test
    void invalidRequestsAreRejectedWithInvalidRequestReason() {
        assertInvalid(null, "alice", 1, ResourceSet.of(1, 1, 0), "空幂等键");
        assertInvalid("k", "  ", 1, ResourceSet.of(1, 1, 0), "空用户");
        assertInvalid("k", "alice", 0, ResourceSet.of(1, 1, 0), "节点数为 0");
        assertInvalid("k", "alice", 1, ResourceSet.of(0, 0, 0), "资源全为 0");
        assertEquals(0, scheduler.listJobs().size(), "非法请求不得入队");
        log.info("[判定] 4 类非法请求全部以 INVALID_REQUEST 拒绝，队列为空且无资源占用");
    }

    @Test
    void unknownUserIsRejectedWithDistinctReason() {
        SchedulerService fresh = new SchedulerService();
        fresh.registerNode("n1", ResourceSet.of(10, 10000, 0));
        SubmissionReport report = fresh.submit("k", "ghost", 1, 1, ResourceSet.of(1, 100, 0));
        log.info("[输入] 未配置配额用户 ghost => status={} reason={}", report.status(), report.reasonCode());
        assertSame(SubmissionStatus.REJECTED, report.status());
        assertEquals(ReasonCode.UNKNOWN_USER, report.reasonCode());
        assertNotEquals(ReasonCode.INVALID_REQUEST, report.reasonCode());
    }

    private void assertInvalid(String key, String user, int nodes, ResourceSet res, String desc) {
        ScheduleException ex = null;
        try {
            scheduler.submit(key, user, 1, nodes, res);
        } catch (ScheduleException e) {
            ex = e;
        }
        assertEquals(ReasonCode.INVALID_REQUEST, ex == null ? null : ex.getReasonCode(),
                "非法请求应抛出 INVALID_REQUEST: " + desc);
        log.info("[判定] 非法请求[{}] => INVALID_REQUEST: {}",
                desc, ex == null ? "" : ex.getMessage());
    }
}

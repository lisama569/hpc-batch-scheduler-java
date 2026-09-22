package com.github.lisama569.hpcbatchschedulerjava.service;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Job;
import com.github.lisama569.hpcbatchschedulerjava.model.JobState;
import com.github.lisama569.hpcbatchschedulerjava.model.PreemptionPlan;
import com.github.lisama569.hpcbatchschedulerjava.model.ReasonCode;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.model.SchedulePlan;
import com.github.lisama569.hpcbatchschedulerjava.model.TaskSpec;
import com.github.lisama569.hpcbatchschedulerjava.quota.QuotaManager;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.GangScheduler;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.PreemptionPlanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 统一的作业服务入口。
 *
 * <p>REST 控制器与进程内调用都经过本类，因此“并发提交、成组调度、抢占”在各入口下结论一致：
 * <ul>
 *   <li>提交：先做请求合法性校验（非法抛 {@link InvalidRequestException}，HTTP 400），
 *       再在同一把全局锁内做幂等键查重（重复抛 {@link DuplicateSubmissionException}，HTTP 409，
 *       返回既有作业，绝不产生第二个实例或重复占资源）；</li>
 *   <li>调度：每轮按“优先级降序、同优先级提交时间升序”处理排队作业，依次经过
 *       配额准入 → 成组试算 → 抢占试算 → 原子提交；任意一步失败都给出可解释依据并保持排队；</li>
 *   <li>完成/抢占：成组资源整体回收，账本容量守恒。</li>
 * </ul>
 */
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final ResourceLedger ledger;
    private final QuotaManager quotaManager;
    private final GangScheduler gangScheduler;
    private final PreemptionPlanner preemptionPlanner;

    private final Object lock = new Object();
    private final Map<String, Job> jobsById = new LinkedHashMap<>();
    private final Map<String, String> idempotencyIndex = new LinkedHashMap<>();
    private final AtomicLong idSequence = new AtomicLong();

    public JobService(ResourceLedger ledger, QuotaManager quotaManager) {
        this.ledger = ledger;
        this.quotaManager = quotaManager;
        this.gangScheduler = new GangScheduler(ledger);
        this.preemptionPlanner = new PreemptionPlanner(ledger);
    }

    /**
     * 提交作业。非法请求与重复请求通过两类不同异常区分；合法但暂时无法运行的作业进入排队。
     */
    public Job submit(String idempotencyKey, String owner, int priority, List<TaskSpec> tasks) {
        validate(idempotencyKey, owner, tasks);

        Job job;
        synchronized (lock) {
            String existingId = idempotencyIndex.get(idempotencyKey);
            if (existingId != null) {
                Job existing = jobsById.get(existingId);
                log.info("提交拒绝: 幂等键={} 重复，命中既有作业={} 状态={}（不重复占用资源）",
                        idempotencyKey, existing.id(), existing.state());
                throw new DuplicateSubmissionException(existing);
            }
            String jobId = "job-" + idSequence.incrementAndGet();
            job = new Job(jobId, idempotencyKey, owner, priority, tasks);
            jobsById.put(jobId, job);
            idempotencyIndex.put(idempotencyKey, jobId);
            log.info("提交接受: jobId={} 幂等键={} owner={} priority={} 任务数={} 总需求={}",
                    jobId, idempotencyKey, owner, priority, tasks.size(), totalRequirement(tasks).describe());

            tryScheduleLocked(job);
        }
        schedulePending();
        return job;
    }

    private void validate(String idempotencyKey, String owner, List<TaskSpec> tasks) {
        List<String> problems = new ArrayList<>();
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            problems.add("idempotencyKey 不能为空");
        }
        if (owner == null || owner.isBlank()) {
            problems.add("owner 不能为空");
        }
        if (tasks == null || tasks.isEmpty()) {
            problems.add("tasks 不能为空，成组作业至少包含一个任务");
        } else {
            for (int i = 0; i < tasks.size(); i++) {
                TaskSpec t = tasks.get(i);
                if (t == null || t.name() == null || t.name().isBlank()) {
                    problems.add("tasks[" + i + "].name 不能为空");
                    continue;
                }
                if (t.requirement() == null) {
                    problems.add("tasks[" + i + "].requirement 不能为空");
                    continue;
                }
                for (var type : com.github.lisama569.hpcbatchschedulerjava.model.ResourceType.values()) {
                    if (t.requirement().get(type) < 0) {
                        problems.add("tasks[" + i + "] " + type + " 需求不能为负");
                    }
                }
            }
        }
        if (!problems.isEmpty()) {
            log.info("提交拒绝: 非法请求 依据={}", problems);
            throw new InvalidRequestException("非法作业请求: " + String.join("; ", problems));
        }
    }

    /** 标记作业完成并整组回收资源，随后尝试调度排队作业。 */
    public Optional<Job> complete(String jobId) {
        synchronized (lock) {
            Job job = jobsById.get(jobId);
            if (job == null) {
                return Optional.empty();
            }
            if (job.state() == JobState.RUNNING) {
                ledger.releaseAtomic(job.tasks(), job.placement());
                job.markPlaced(Map.of());
                job.setState(JobState.COMPLETED);
                job.recordDecision(ReasonCode.SCHEDULED, List.of("作业完成，已整组回收资源: " + job.placement()));
                log.info("作业完成: {} 资源已回收", jobId);
            }
        }
        schedulePending();
        return Optional.ofNullable(jobsById.get(jobId));
    }

    public Optional<Job> findJob(String jobId) {
        synchronized (lock) {
            return Optional.ofNullable(jobsById.get(jobId));
        }
    }

    public List<Job> listJobs() {
        synchronized (lock) {
            return new ArrayList<>(jobsById.values());
        }
    }

    /** 供节点运维服务共享调度锁，保证排空驱逐与提交/调度互斥。 */
    public Object schedulingLock() {
        return lock;
    }

    /**
     * 执行一轮调度：按优先级/提交时间排序所有 QUEUED 作业并尝试落地。
     *
     * @return 本轮新启动的作业数
     */
    public int schedulePending() {
        int started = 0;
        synchronized (lock) {
            List<Job> pending = jobsById.values().stream()
                    .filter(j -> j.state() == JobState.QUEUED)
                    .sorted(Comparator.comparingInt(Job::priority).reversed()
                            .thenComparing(Job::submittedAtNanos)
                            .thenComparing(Job::id))
                    .toList();
            for (Job job : pending) {
                if (tryScheduleLocked(job)) {
                    started++;
                }
            }
        }
        if (started > 0) {
            log.info("调度轮结束: 本轮启动 {} 个作业，账本校验 {}",
                    started, ledger.verifyInvariants().isEmpty() ? "守恒" : ledger.verifyInvariants());
        }
        return started;
    }

    /**
     * 在全局锁内尝试调度单个作业：配额准入 → 成组试算 → 抢占试算 → 原子提交。
     * 必须在持有 {@link #lock} 时调用。
     */
    private boolean tryScheduleLocked(Job job) {
        List<String> basis = new ArrayList<>();
        Resources total = totalRequirement(job.tasks());
        basis.add("调度判定作业 " + job.id() + " owner=" + job.owner()
                + " priority=" + job.priority() + " 总需求=" + total.describe());

        // 1) 配额准入
        List<String> quotaReasons = new ArrayList<>();
        if (!quotaManager.admits(job.owner(), total, quotaReasons)) {
            basis.addAll(quotaReasons);
            basis.add("结论: 超过属主配额，作业保持 QUEUED（未申请任何节点资源）");
            job.recordDecision(ReasonCode.QUOTA_EXCEEDED, basis);
            log.info("调度: 作业={} QUOTA_EXCEEDED 依据={}", job.id(), quotaReasons);
            return false;
        }
        basis.add("配额准入通过");

        // 2) 成组试算
        SchedulePlan plan = gangScheduler.evaluate(job);
        if (plan.feasible()) {
            List<String> commitLog = new ArrayList<>();
            if (gangScheduler.commit(job, plan, commitLog)) {
                basis.add("成组试算通过并原子提交成功");
                basis.addAll(commitLog);
                job.recordDecision(ReasonCode.SCHEDULED, basis);
                return true;
            }
            basis.addAll(commitLog);
            job.recordDecision(ReasonCode.INSUFFICIENT_CAPACITY, basis);
            return false;
        }
        basis.addAll(plan.reasons());

        // 3) 抢占试算（只抢严格更低优先级，且必须能让本作业完整落地）
        List<Job> runningJobs = jobsById.values().stream()
                .filter(j -> j.state() == JobState.RUNNING)
                .toList();
        PreemptionPlan preemption = preemptionPlanner.evaluate(job, runningJobs);
        if (!preemption.feasible()) {
            basis.addAll(preemption.reasons());
            basis.add("结论: 容量不足且无可行抢占，作业保持 QUEUED，无任何资源变动");
            job.recordDecision(ReasonCode.INSUFFICIENT_CAPACITY, basis);
            log.info("调度: 作业={} INSUFFICIENT_CAPACITY（无可行抢占）", job.id());
            return false;
        }

        // 4) 先回收受害者资源（整组），受害者重新排队，再原子提交目标作业
        basis.addAll(preemption.reasons());
        for (String victimId : preemption.victimJobIds()) {
            Job victim = jobsById.get(victimId);
            ledger.releaseAtomic(victim.tasks(), victim.placement());
            victim.markPreempted(job.id(), List.of(
                    "被作业 " + job.id() + "(priority=" + job.priority() + ") 抢占："
                            + "本作业 priority=" + victim.priority() + " 严格更低；占用资源已整组回收，重新排队"));
            basis.add("已抢占 " + victimId + " 并完整回收其资源");
            log.info("抢占: {} 被 {} 抢占，资源已回收并重新排队", victimId, job.id());
        }
        List<String> commitLog = new ArrayList<>();
        SchedulePlan preemptivePlan = new SchedulePlan(true, preemption.placement(), List.of(), Map.of());
        if (!gangScheduler.commit(job, preemptivePlan, commitLog)) {
            // 理论上不会发生（试算刚刚通过）；发生则保持账本一致并报告
            basis.addAll(commitLog);
            job.recordDecision(ReasonCode.INSUFFICIENT_CAPACITY, basis);
            return false;
        }
        basis.addAll(commitLog);
        job.recordDecision(ReasonCode.SCHEDULED, basis);
        log.info("调度: 作业={} 经抢占启动，受害者={}", job.id(), preemption.victimJobIds());
        return true;
    }

    private Resources totalRequirement(List<TaskSpec> tasks) {
        Resources total = Resources.empty();
        for (TaskSpec task : tasks) {
            total = total.plus(task.requirement());
        }
        return total;
    }

    /** 仅供测试/装配生成唯一标识，未使用时保留以备扩展。 */
    @SuppressWarnings("unused")
    private String newUuid() {
        return UUID.randomUUID().toString();
    }
}

package com.github.lisama569.hpcbatchschedulerjava.scheduler;

import com.github.lisama569.hpcbatchschedulerjava.domain.ClusterNode;
import com.github.lisama569.hpcbatchschedulerjava.domain.Job;
import com.github.lisama569.hpcbatchschedulerjava.domain.JobState;
import com.github.lisama569.hpcbatchschedulerjava.domain.NodeState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType;
import com.github.lisama569.hpcbatchschedulerjava.domain.UserQuota;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 调度与资源分配核心服务。
 *
 * 所有判定（幂等、配额、成组调度、抢占）都在同一把重入锁内串行完成，
 * 因此 HTTP 入口与进程内入口得到的结论完全一致，且资源账本始终守恒。
 */
public final class SchedulerService {

    private static final Logger log = LoggerFactory.getLogger(SchedulerService.class);

    private final ReentrantLock lock = new ReentrantLock(true);
    private final ResourceLedger ledger = new ResourceLedger();

    private final Map<String, Job> jobsById = new LinkedHashMap<>();
    private final Map<String, String> idempotencyIndex = new LinkedHashMap<>();
    private final Map<String, UserQuota> quotas = new LinkedHashMap<>();

    private final AtomicLong jobSequence = new AtomicLong(0);

    public SchedulerService() {
    }

    // ================= 集群与配额管理 =================

    public boolean registerNode(String nodeId, ResourceSet capacity) {
        Objects.requireNonNull(capacity, "capacity");
        lock.lock();
        try {
            if (ledger.getNode(nodeId) != null) {
                log.info("register-node rejected: node already exists nodeId={}", nodeId);
                return false;
            }
            boolean ok = ledger.registerNode(nodeId, capacity);
            log.info("register-node nodeId={} capacity={} result={}", nodeId, capacity, ok);
            return ok;
        } finally {
            lock.unlock();
        }
    }

    public boolean setNodeState(String nodeId, NodeState state) {
        Objects.requireNonNull(state, "state");
        lock.lock();
        try {
            boolean ok = ledger.setState(nodeId, state);
            log.info("set-node-state nodeId={} state={} result={}", nodeId, state, ok);
            return ok;
        } finally {
            lock.unlock();
        }
    }

    public ClusterNode getNode(String nodeId) {
        lock.lock();
        try {
            return ledger.getNode(nodeId);
        } finally {
            lock.unlock();
        }
    }

    public List<ClusterNode> listNodes() {
        lock.lock();
        try {
            return ledger.allNodes();
        } finally {
            lock.unlock();
        }
    }

    public void setQuota(UserQuota quota) {
        Objects.requireNonNull(quota, "quota");
        lock.lock();
        try {
            if (quota.maxRunningJobs() < 0 || quota.maxCpu() < 0 || quota.maxMemoryMb() < 0) {
                throw new ScheduleException(ReasonCode.INVALID_REQUEST, "quota values must be non-negative");
            }
            quotas.put(quota.user(), quota);
            log.info("set-quota user={} maxRunningJobs={} maxCpu={} maxMemoryMb={}",
                    quota.user(), quota.maxRunningJobs(), quota.maxCpu(), quota.maxMemoryMb());
        } finally {
            lock.unlock();
        }
    }

    public UserQuota getQuota(String user) {
        lock.lock();
        try {
            return quotas.get(user);
        } finally {
            lock.unlock();
        }
    }

    // ================= 作业提交（幂等） =================

    /**
     * 提交作业（无前置依赖），等价于 {@link #submit(String, String, int, int, ResourceSet, List)}。
     */
    public SubmissionReport submit(String idempotencyKey, String user, int priority,
                                   int nodeCount, ResourceSet perNode) {
        return submit(idempotencyKey, user, priority, nodeCount, perNode, List.of());
    }

    /**
     * 提交作业。相同幂等键的并发/重复提交只会产生一个实例：
     * 内容一致 => 返回既有作业（DUPLICATE，不重复占资源、不重复挂依赖）；
     * 内容冲突 => 拒绝（IDEMPOTENCY_KEY_CONFLICT），与普通重复可区分。
     *
     * @param dependsOn 前置作业引用（jobId 或提交时的幂等键），全部成功完成本作业才可被调度
     */
    public SubmissionReport submit(String idempotencyKey, String user, int priority,
                                   int nodeCount, ResourceSet perNode, List<String> dependsOn) {
        // 非法请求在锁前即可判定。
        List<String> depRefs = validateRequest(idempotencyKey, user, nodeCount, perNode, dependsOn);

        lock.lock();
        try {
            String existingId = idempotencyIndex.get(idempotencyKey);
            if (existingId != null) {
                Job existing = jobsById.get(existingId);
                if (sameContent(existing, user, priority, nodeCount, perNode, resolveLenient(depRefs))) {
                    log.info("submit duplicate idempotencyKey={} jobId={} state={}（不重复占用资源、不重复挂依赖）",
                            idempotencyKey, existing.getJobId(), existing.getState());
                    return report(existing, SubmissionStatus.DUPLICATE, ReasonCode.DUPLICATE_SUBMISSION,
                            "duplicate submission with idempotency key " + idempotencyKey
                                    + "; existing job " + existing.getJobId() + " in state " + existing.getState());
                }
                log.warn("submit conflict idempotencyKey={} existingJob={} 新内容与既有内容不一致",
                        idempotencyKey, existingId);
                return report(existing, SubmissionStatus.REJECTED, ReasonCode.IDEMPOTENCY_KEY_CONFLICT,
                        "idempotency key " + idempotencyKey + " already used by job "
                                + existingId + " with a different payload");
            }

            UserQuota quota = quotas.get(user);
            if (quota == null) {
                log.warn("submit rejected idempotencyKey={} user={} reason=UNKNOWN_USER", idempotencyKey, user);
                return rejectedReport(null, ReasonCode.UNKNOWN_USER,
                        "user '" + user + "' has no quota configured");
            }

            // 依赖校验：自引用成环 / 引用不存在的作业，在提交时明确拒绝且原因可区分。
            List<String> depIds = new ArrayList<>();
            for (String ref : depRefs) {
                if (ref.equals(idempotencyKey)) {
                    log.warn("submit rejected idempotencyKey={} reason=DEPENDENCY_CYCLE 自引用", idempotencyKey);
                    return rejectedReport(null, ReasonCode.DEPENDENCY_CYCLE,
                            "dependency cycle: job references its own idempotency key '" + ref + "'");
                }
                String depId = resolveJobId(ref);
                if (depId == null) {
                    log.warn("submit rejected idempotencyKey={} reason=DEPENDENCY_NOT_FOUND ref={}",
                            idempotencyKey, ref);
                    return rejectedReport(null, ReasonCode.DEPENDENCY_NOT_FOUND,
                            "dependency not found: '" + ref + "' is neither a known jobId nor a known idempotency key");
                }
                if (!depIds.contains(depId)) {
                    depIds.add(depId);
                }
            }
            String jobId = "job-" + (jobSequence.get() + 1);
            if (createsCycle(jobId, depIds)) {
                log.warn("submit rejected idempotencyKey={} reason=DEPENDENCY_CYCLE deps={}",
                        idempotencyKey, depIds);
                return rejectedReport(null, ReasonCode.DEPENDENCY_CYCLE,
                        "dependency cycle detected via " + depIds);
            }

            jobId = "job-" + jobSequence.incrementAndGet();
            Job job = new Job(jobId, idempotencyKey, user, priority, nodeCount, perNode,
                    System.currentTimeMillis());
            job.setDependsOnJobIds(List.copyOf(depIds));
            jobsById.put(jobId, job);
            idempotencyIndex.put(idempotencyKey, jobId);
            log.info("submit accepted jobId={} idempotencyKey={} user={} priority={} nodes={} perNode={} dependsOn={}",
                    jobId, idempotencyKey, user, priority, nodeCount, perNode, depIds);

            ReasonCode code = depIds.isEmpty() ? scheduleOne(job) : evaluateDependencies(job);
            return report(job, SubmissionStatus.ACCEPTED, code,
                    job.getState() == JobState.RUNNING
                            ? "job scheduled immediately on " + job.getAssignedNodeIds()
                            : job.getLastReason());
        } finally {
            lock.unlock();
        }
    }

    private List<String> validateRequest(String idempotencyKey, String user, int nodeCount,
                                         ResourceSet perNode, List<String> dependsOn) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new ScheduleException(ReasonCode.INVALID_REQUEST, "idempotencyKey must not be blank");
        }
        if (user == null || user.isBlank()) {
            throw new ScheduleException(ReasonCode.INVALID_REQUEST, "user must not be blank");
        }
        if (nodeCount <= 0) {
            throw new ScheduleException(ReasonCode.INVALID_REQUEST, "nodeCount must be positive, got " + nodeCount);
        }
        if (perNode == null || perNode.isEmpty()) {
            throw new ScheduleException(ReasonCode.INVALID_REQUEST, "per-node resource request must not be empty");
        }
        List<String> refs = dependsOn == null ? List.of() : dependsOn;
        List<String> deduped = new ArrayList<>();
        for (String ref : refs) {
            if (ref == null || ref.isBlank()) {
                throw new ScheduleException(ReasonCode.INVALID_REQUEST,
                        "dependency reference must not be blank");
            }
            if (!deduped.contains(ref)) {
                deduped.add(ref);
            }
        }
        return deduped;
    }

    /** 依赖引用解析：既接受 jobId，也接受提交时的幂等键。 */
    private String resolveJobId(String ref) {
        if (jobsById.containsKey(ref)) {
            return ref;
        }
        return idempotencyIndex.get(ref);
    }

    /** 宽松解析（幂等比较用）：能解析则换成 jobId，否则保留原引用（必然与既有内容不等）。 */
    private List<String> resolveLenient(List<String> refs) {
        List<String> ids = new ArrayList<>();
        for (String ref : refs) {
            String resolved = resolveJobId(ref);
            String id = resolved != null ? resolved : ref;
            if (!ids.contains(id)) {
                ids.add(id);
            }
        }
        return ids;
    }

    /**
     * 成环防护：从各前置沿依赖边遍历，若能回到新作业自身则成环。
     * 当前实现下新作业尚无任何后继，间接环结构上不可达，本检查作为不变量防线保留。
     */
    private boolean createsCycle(String newJobId, List<String> depIds) {
        Set<String> visited = new HashSet<>();
        Deque<String> stack = new ArrayDeque<>(depIds);
        while (!stack.isEmpty()) {
            String current = stack.poll();
            if (current.equals(newJobId)) {
                return true;
            }
            if (!visited.add(current)) {
                continue;
            }
            Job job = jobsById.get(current);
            if (job != null) {
                stack.addAll(job.getDependsOnJobIds());
            }
        }
        return false;
    }

    private boolean sameContent(Job existing, String user, int priority, int nodeCount,
                                ResourceSet perNode, List<String> depIds) {
        return existing.getUser().equals(user)
                && existing.getPriority() == priority
                && existing.getNodeCount() == nodeCount
                && existing.getPerNodeResource().asMap().equals(perNode.asMap())
                && existing.getDependsOnJobIds().equals(depIds);
    }

    // ================= 作业查询与生命周期 =================

    public Job getJob(String jobId) {
        lock.lock();
        try {
            return jobsById.get(jobId);
        } finally {
            lock.unlock();
        }
    }

    public List<Job> listJobs() {
        lock.lock();
        try {
            return new ArrayList<>(jobsById.values());
        } finally {
            lock.unlock();
        }
    }

    /** 正常完成：只有运行中作业可完成，完整释放资源、向下游传播成功结局后扫描队列。 */
    public boolean completeJob(String jobId) {
        lock.lock();
        try {
            Job job = requireJob(jobId);
            if (job.getState() != JobState.RUNNING) {
                throw new ScheduleException(ReasonCode.JOB_STATE_CONFLICT,
                        "job " + jobId + " is " + job.getState() + ", only RUNNING jobs can complete");
            }
            releaseRunningJob(job, JobState.COMPLETED, null);
            log.info("complete jobId={} released={} on {} 个节点", jobId, job.getPerNodeResource(),
                    job.getAssignedNodeIds().size());
            propagateToDependents(jobId);
            runSchedulePass();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** 取消：排队/等待中作业直接取消（无资源）；运行中作业取消并释放资源。取消结局会向下游传播。 */
    public boolean cancelJob(String jobId) {
        lock.lock();
        try {
            Job job = requireJob(jobId);
            JobState state = job.getState();
            if (state == JobState.COMPLETED || state == JobState.CANCELLED
                    || state == JobState.PREEMPTED || state == JobState.DEPENDENCY_FAILED) {
                throw new ScheduleException(ReasonCode.JOB_STATE_CONFLICT,
                        "job " + jobId + " already terminal: " + state);
            }
            if (state == JobState.RUNNING) {
                releaseRunningJob(job, JobState.CANCELLED, null);
                job.setLastReason("cancelled by user while running");
                log.info("cancel running jobId={} 已释放全部资源", jobId);
                propagateToDependents(jobId);
                runSchedulePass();
            } else {
                job.setState(JobState.CANCELLED);
                job.setLastReason("cancelled by user while " + state.name().toLowerCase());
                log.info("cancel {} jobId={}", state.name().toLowerCase(), jobId);
                propagateToDependents(jobId);
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void releaseRunningJob(Job job, JobState targetState, String preemptedBy) {
        ledger.releaseJob(job.getJobId(), job.getAssignedNodeIds(), job.getPerNodeResource());
        List<String> released = job.getAssignedNodeIds();
        job.setAssignedNodeIds(List.of());
        job.setState(targetState);
        job.setPreemptedByJobId(preemptedBy);
        log.info("release jobId={} nodes={} state={} 账本一致={} 全局已用={}",
                job.getJobId(), released, targetState, ledger.isConsistent(), ledger.totalUsed());
    }

    private Job requireJob(String jobId) {
        Job job = jobsById.get(jobId);
        if (job == null) {
            throw new ScheduleException(ReasonCode.JOB_NOT_FOUND, "job not found: " + jobId);
        }
        return job;
    }

    // ================= 依赖编排 =================

    /** 前置作业未成功结束的终态集合（取消/被抢占/前置失败级联）。 */
    private static final Set<JobState> DEP_FAILURE_STATES = EnumSet.of(
            JobState.CANCELLED, JobState.PREEMPTED, JobState.DEPENDENCY_FAILED);

    /**
     * 提交时评估依赖（只调用一次，job 尚为初始 QUEUED 态）：
     * 有失败前置 => 直接落 DEPENDENCY_FAILED 终态；
     * 全部前置已 COMPLETED => 保持 QUEUED 并立即尝试调度；
     * 否则 => WAITING（不占资源、不计配额、不参与调度扫描）。
     */
    private ReasonCode evaluateDependencies(Job job) {
        Job failedDep = firstFailedDependency(job);
        if (failedDep != null) {
            failOnDependency(job, failedDep);
            return ReasonCode.DEPENDENCY_FAILED;
        }
        List<String> pending = pendingDependencies(job);
        if (pending.isEmpty()) {
            log.info("dependencies-already-met jobId={} 全部前置已 COMPLETED，直接进入调度", job.getJobId());
            return scheduleOne(job);
        }
        waitOnDependencies(job, pending);
        return ReasonCode.WAITING_ON_DEPENDENCIES;
    }

    private void waitOnDependencies(Job job, List<String> pending) {
        job.setState(JobState.WAITING);
        job.setLastReason("[WAITING_ON_DEPENDENCIES] waiting for: " + String.join(", ", pending));
        log.info("wait-on-dependencies jobId={} pending={}（不占资源、不计配额、不参与调度扫描）",
                job.getJobId(), pending);
    }

    private void failOnDependency(Job job, Job failedDep) {
        job.setState(JobState.DEPENDENCY_FAILED);
        job.setLastReason("[DEPENDENCY_FAILED] dependency " + failedDep.getJobId()
                + " ended as " + failedDep.getState()
                + " (" + failedDep.getLastReason() + ")");
        log.info("dependency-failed jobId={} 前置={} 前置结局={}", job.getJobId(),
                failedDep.getJobId(), failedDep.getState());
    }

    private Job firstFailedDependency(Job job) {
        for (String depId : job.getDependsOnJobIds()) {
            Job dep = jobsById.get(depId);
            if (dep != null && DEP_FAILURE_STATES.contains(dep.getState())) {
                return dep;
            }
        }
        return null;
    }

    /** 尚未 COMPLETED 的前置列表（含各自当前状态，用于可解释排队原因）。 */
    private List<String> pendingDependencies(Job job) {
        List<String> pending = new ArrayList<>();
        for (String depId : job.getDependsOnJobIds()) {
            Job dep = jobsById.get(depId);
            if (dep != null && dep.getState() != JobState.COMPLETED) {
                pending.add(depId + "(" + dep.getState() + ")");
            }
        }
        return pending;
    }

    /**
     * 前置作业进入终态后自动向下游传播（BFS，按提交顺序扫描，结论确定）：
     * 全部前置成功 => 解锁为 QUEUED（由随后的调度轮次按统一优先级规则启动）；
     * 任一前置失败 => 落 DEPENDENCY_FAILED 终态并继续级联，不会无声悬挂。
     */
    private void propagateToDependents(String finishedJobId) {
        Deque<String> queue = new ArrayDeque<>();
        queue.add(finishedJobId);
        while (!queue.isEmpty()) {
            String doneId = queue.poll();
            for (Job w : jobsById.values()) {
                if (w.getState() != JobState.WAITING || !w.getDependsOnJobIds().contains(doneId)) {
                    continue;
                }
                Job failedDep = firstFailedDependency(w);
                if (failedDep != null) {
                    failOnDependency(w, failedDep);
                    queue.add(w.getJobId());
                    continue;
                }
                List<String> pending = pendingDependencies(w);
                if (pending.isEmpty()) {
                    w.setState(JobState.QUEUED);
                    w.setLastReason(null);
                    log.info("dependency-unlocked jobId={} 全部前置已 COMPLETED => QUEUED", w.getJobId());
                } else {
                    waitOnDependencies(w, pending);
                }
            }
        }
    }

    // ================= 调度核心 =================

    /** 显式触发调度扫描，返回本次新启动的作业数。 */
    public int scheduleTick() {
        lock.lock();
        try {
            return runSchedulePass();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 一轮调度：优先级高者、同优先级先到先得。每个作业独立尝试放置/抢占。
     * 单轮结束即返回（资源释放后会由 complete/cancel/tick 再次触发）。
     */
    private int runSchedulePass() {
        List<Job> queue = jobsById.values().stream()
                .filter(j -> j.getState() == JobState.QUEUED)
                .sorted(Comparator.comparingInt(Job::getPriority).reversed()
                        .thenComparingLong(Job::getSubmittedAtMillis))
                .toList();
        int started = 0;
        for (Job job : queue) {
            if (job.getState() == JobState.QUEUED && tryStart(job) == ReasonCode.NONE) {
                started++;
            }
        }
        log.info("schedule-pass done started={} remainingQueued={} 账本一致={}",
                started, countQueued(), ledger.isConsistent());
        return started;
    }

    private int countQueued() {
        int n = 0;
        for (Job job : jobsById.values()) {
            if (job.getState() == JobState.QUEUED) {
                n++;
            }
        }
        return n;
    }

    private ReasonCode scheduleOne(Job job) {
        return tryStart(job);
    }

    /**
     * 尝试启动单个排队作业：配额 -> 普通成组放置 -> 抢占式放置。
     * 返回 NONE 表示已启动，否则返回排队的可解释原因码。
     */
    private ReasonCode tryStart(Job job) {
        ReasonCode quota = checkQuota(job);
        if (quota != ReasonCode.NONE) {
            stayQueued(job, quota, explainQuota(job, quota));
            return quota;
        }

        PlacementOutcome placement = findPlacement(job, ledger.schedulableNodes());
        if (placement.success()) {
            commitPlacement(job, placement.selectedNodeIds(), List.of());
            return ReasonCode.NONE;
        }

        // 容量硬上限不满足时抢占也无意义。
        if (placement.explanation() != null && placement.explanation().startsWith("NO_FITTING_NODE")) {
            stayQueued(job, ReasonCode.NO_FITTING_NODE, placement.explanation());
            return ReasonCode.NO_FITTING_NODE;
        }

        PreemptivePlan plan = tryPlanPreemption(job);
        if (plan == null) {
            ReasonCode code = hasLowerPriorityRunning(job)
                    ? ReasonCode.PREEMPTION_INSUFFICIENT : ReasonCode.INSUFFICIENT_CAPACITY;
            stayQueued(job, code, explainInsufficient(job, placement, code));
            return code;
        }

        // 先回收被抢占作业的全部资源，再原子成组分配；分配失败则抛出（账本不变量被破坏）。
        for (PreemptionRecord record : plan.records()) {
            Job victim = jobsById.get(record.preemptedJobId());
            ledger.releaseJob(victim.getJobId(), victim.getAssignedNodeIds(), victim.getPerNodeResource());
            victim.setAssignedNodeIds(List.of());
            victim.setState(JobState.PREEMPTED);
            victim.setPreemptedByJobId(job.getJobId());
            victim.setLastReason("preempted by " + job.getJobId()
                    + " (victim priority " + record.preemptedPriority()
                    + " < requester priority " + record.requesterPriority() + "): " + record.basis());
            log.info("preempt victim={} by={} nodesReleased={} basis={}",
                    victim.getJobId(), job.getJobId(), record.nodesReleased(), record.basis());
            propagateToDependents(victim.getJobId());
        }
        boolean allocated = ledger.allocateGang(plan.selectedNodeIds(), job.getJobId(), job.getPerNodeResource());
        if (!allocated) {
            throw new IllegalStateException("preemption planned but gang allocation failed for " + job.getJobId());
        }
        commitPlacementState(job, plan.selectedNodeIds());
        log.info("start-after-preemption jobId={} nodes={} preempted={}",
                job.getJobId(), plan.selectedNodeIds(), plan.records().stream()
                        .map(PreemptionRecord::preemptedJobId).toList());
        return ReasonCode.NONE;
    }

    private void stayQueued(Job job, ReasonCode code, String explanation) {
        job.setState(JobState.QUEUED);
        job.setLastReason("[" + code.name() + "] " + explanation);
        log.info("stay-queued jobId={} reason={} basis={}", job.getJobId(), code, explanation);
    }

    private void commitPlacement(Job job, List<String> nodeIds, List<PreemptionRecord> records) {
        boolean ok = ledger.allocateGang(nodeIds, job.getJobId(), job.getPerNodeResource());
        if (!ok) {
            throw new IllegalStateException("placement found but allocation failed for " + job.getJobId());
        }
        commitPlacementState(job, nodeIds);
    }

    private void commitPlacementState(Job job, List<String> nodeIds) {
        job.setAssignedNodeIds(new ArrayList<>(nodeIds));
        job.setState(JobState.RUNNING);
        job.setLastReason(null);
        log.info("start jobId={} user={} priority={} nodes={} perNode={} 全局已用={}",
                job.getJobId(), job.getUser(), job.getPriority(), nodeIds, job.getPerNodeResource(),
                ledger.totalUsed());
    }

    /**
     * 配额判定（只统计当前 RUNNING 作业；排队作业不计入占用）。
     */
    private ReasonCode checkQuota(Job candidate) {
        UserQuota quota = quotas.get(candidate.getUser());
        if (quota == null) {
            return ReasonCode.UNKNOWN_USER;
        }
        int running = 0;
        long cpu = 0;
        long memory = 0;
        for (Job j : jobsById.values()) {
            if (j.getState() == JobState.RUNNING && j.getUser().equals(candidate.getUser())) {
                running++;
                cpu += j.getPerNodeResource().get(ResourceType.CPU) * j.getNodeCount();
                memory += j.getPerNodeResource().get(ResourceType.MEMORY_MB) * j.getNodeCount();
            }
        }
        long addCpu = candidate.getPerNodeResource().get(ResourceType.CPU) * candidate.getNodeCount();
        long addMem = candidate.getPerNodeResource().get(ResourceType.MEMORY_MB) * candidate.getNodeCount();
        if (running + 1 > quota.maxRunningJobs()) {
            return ReasonCode.QUOTA_JOB_LIMIT;
        }
        if (cpu + addCpu > quota.maxCpu()) {
            return ReasonCode.QUOTA_CPU_LIMIT;
        }
        if (memory + addMem > quota.maxMemoryMb()) {
            return ReasonCode.QUOTA_MEMORY_LIMIT;
        }
        return ReasonCode.NONE;
    }

    private String explainQuota(Job job, ReasonCode code) {
        UserQuota q = quotas.get(job.getUser());
        return switch (code) {
            case QUOTA_JOB_LIMIT -> "user " + job.getUser() + " running job count would exceed limit "
                    + q.maxRunningJobs();
            case QUOTA_CPU_LIMIT -> "user " + job.getUser() + " total running CPU would exceed limit "
                    + q.maxCpu();
            case QUOTA_MEMORY_LIMIT -> "user " + job.getUser() + " total running memory would exceed limit "
                    + q.maxMemoryMb() + "MB";
            default -> code.name();
        };
    }

    /**
     * 普通成组放置：必须一次性选出 nodeCount 个健康且各自满足 perNode 的节点。
     * 选择策略：可用资源最贴近需求的节点优先（best-fit），为大需求保留充裕节点。
     */
    private PlacementOutcome findPlacement(Job job, List<ClusterNode> candidates) {
        ResourceSet need = job.getPerNodeResource();

        // 硬上限：容量本身装不下的节点永远不可用（排空恢复/抢占都改变不了）。
        List<ClusterNode> fitting = new ArrayList<>();
        List<String> oversized = new ArrayList<>();
        for (ClusterNode node : candidates) {
            if (ResourceSet.dominates(node.getCapacity(), need)) {
                fitting.add(node);
            } else {
                oversized.add(node.getNodeId());
            }
        }
        if (fitting.size() < job.getNodeCount()) {
            String why = "NO_FITTING_NODE: 需要 " + job.getNodeCount()
                    + " 个容量>= " + need + " 的健康节点，实际满足容量上限的只有 "
                    + fitting.size() + " 个；不可参与: " + oversized;
            return PlacementOutcome.fail(shortfallsByNode(fitting, need), why);
        }

        List<ClusterNode> ready = fitting.stream()
                .filter(n -> ResourceSet.dominates(n.available(), need))
                .sorted(bestFitComparator(need))
                .toList();

        if (ready.size() < job.getNodeCount()) {
            String why = "INSUFFICIENT_CAPACITY: 需要 " + job.getNodeCount()
                    + " 个节点同时各自腾出 " + need + "，当前立即可用 " + ready.size() + " 个";
            return PlacementOutcome.fail(shortfallsByNode(fitting, need), why);
        }

        List<String> chosen = ready.subList(0, job.getNodeCount()).stream()
                .map(ClusterNode::getNodeId).toList();
        return PlacementOutcome.ok(chosen,
                "found " + chosen.size() + " healthy nodes each satisfying " + need);
    }

    private Comparator<ClusterNode> bestFitComparator(ResourceSet need) {
        return Comparator
                .comparingLong((ClusterNode n) -> -slackScore(n.available(), need))
                .thenComparing(ClusterNode::getNodeId);
    }

    /** 剩余量相对需求的总“富余分”，越小越贴合。 */
    private long slackScore(ResourceSet available, ResourceSet need) {
        long score = 0;
        for (ResourceType type : ResourceType.values()) {
            if (need.get(type) > 0) {
                score += available.get(type) - need.get(type);
            }
        }
        return score;
    }

    private Map<String, Map<String, Long>> shortfallsByNode(List<ClusterNode> nodes, ResourceSet need) {
        Map<String, Map<String, Long>> gaps = new LinkedHashMap<>();
        for (ClusterNode node : nodes) {
            Map<String, Long> gap = ResourceLedger.shortfall(need, node.available());
            if (!gap.isEmpty()) {
                gaps.put(node.getNodeId(), gap);
            }
        }
        return gaps;
    }

    private String explainInsufficient(Job job, PlacementOutcome placement, ReasonCode code) {
        StringBuilder sb = new StringBuilder();
        sb.append(code == ReasonCode.PREEMPTION_INSUFFICIENT
                ? "即使抢占全部更低优先级作业，仍" : "");
        sb.append("无法在 ").append(job.getNodeCount())
                .append(" 个节点上同时满足 ").append(job.getPerNodeResource());
        if (!placement.nodeShortfalls().isEmpty()) {
            sb.append("；逐节点缺额: ").append(placement.nodeShortfalls());
        }
        return sb.toString();
    }

    // ---------------- 抢占规划 ----------------

    private boolean hasLowerPriorityRunning(Job requester) {
        for (Job j : jobsById.values()) {
            if (j.getState() == JobState.RUNNING && j.getPriority() < requester.getPriority()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 抢占式放置规划（只模拟、不落账）：
     * 每轮挑选一个“抢占后能解锁最多合格节点”的更低优先级运行作业；
     * 凑齐 nodeCount 个可用合格节点即成功，否则返回 null（且不产生任何变更）。
     * 严格前提：受害者优先级必须严格低于请求者。
     */
    private PreemptivePlan tryPlanPreemption(Job requester) {
        ResourceSet need = requester.getPerNodeResource();

        // 模拟的各节点可用资源 = 真实可用 + 其上“已计划抢占”作业将释放的量。
        Map<String, ResourceSet> simulatedAvailable = new LinkedHashMap<>();
        Map<String, List<Job>> jobsOnNode = new LinkedHashMap<>();
        for (ClusterNode node : ledger.schedulableNodes()) {
            if (!ResourceSet.dominates(node.getCapacity(), need)) {
                continue;
            }
            simulatedAvailable.put(node.getNodeId(), node.available());
            jobsOnNode.put(node.getNodeId(), new ArrayList<>());
        }
        for (Job running : jobsById.values()) {
            if (running.getState() != JobState.RUNNING) {
                continue;
            }
            for (String nodeId : running.getAssignedNodeIds()) {
                jobsOnNode.computeIfPresent(nodeId, (id, list) -> {
                    list.add(running);
                    return list;
                });
            }
        }

        List<PreemptionRecord> records = new ArrayList<>();
        List<String> selected;

        while (true) {
            selected = collectReadyNodes(requester, need, simulatedAvailable);
            if (selected.size() >= requester.getNodeCount()) {
                return new PreemptivePlan(
                        selected.subList(0, requester.getNodeCount()), records);
            }

            Job victim = chooseBestVictim(requester, need, simulatedAvailable, jobsOnNode);
            if (victim == null) {
                return null;
            }

            List<String> releasedNodes = new ArrayList<>();
            for (String nodeId : victim.getAssignedNodeIds()) {
                ResourceSet current = simulatedAvailable.get(nodeId);
                if (current != null) {
                    ResourceSet after = ResourceSet.sum(current, victim.getPerNodeResource());
                    simulatedAvailable.put(nodeId, after);
                    releasedNodes.add(nodeId);
                }
            }
            for (String nodeId : victim.getAssignedNodeIds()) {
                jobsOnNode.computeIfPresent(nodeId, (id, list) -> {
                    list.remove(victim);
                    return list;
                });
            }
            String basis = "priority " + victim.getPriority() + " < requester "
                    + requester.getPriority() + "; 抢占后可解锁节点 " + releasedNodes;
            records.add(new PreemptionRecord(victim.getJobId(), victim.getPriority(),
                    requester.getJobId(), requester.getPriority(), releasedNodes, basis));
        }
    }

    private List<String> collectReadyNodes(Job requester, ResourceSet need,
                                           Map<String, ResourceSet> simulatedAvailable) {
        List<String> ready = new ArrayList<>();
        for (Map.Entry<String, ResourceSet> e : simulatedAvailable.entrySet()) {
            if (ResourceSet.dominates(e.getValue(), need)) {
                ready.add(e.getKey());
            }
        }
        ready.sort(Comparator.comparingLong((String id) -> -slackScore(simulatedAvailable.get(id), need))
                .thenComparing(id -> id));
        return ready;
    }

    private Job chooseBestVictim(Job requester, ResourceSet need,
                                 Map<String, ResourceSet> simulatedAvailable,
                                 Map<String, List<Job>> jobsOnNode) {
        Job best = null;
        int bestUnlocks = -1;
        long bestSubmittingAt = Long.MAX_VALUE;
        String bestId = null;

        for (Map.Entry<String, List<Job>> e : jobsOnNode.entrySet()) {
            String nodeId = e.getKey();
            if (ResourceSet.dominates(simulatedAvailable.get(nodeId), need)) {
                continue; // 该节点已就绪，不需要在此抢占
            }
            for (Job victim : e.getValue()) {
                if (victim.getPriority() >= requester.getPriority()) {
                    continue; // 只允许抢占严格更低优先级
                }
                int unlocks = countUnlocks(victim, need, simulatedAvailable);
                boolean better = false;
                if (unlocks > bestUnlocks) {
                    better = true;
                } else if (unlocks == bestUnlocks && victim.getPriority() < (best == null
                        ? Integer.MAX_VALUE : best.getPriority())) {
                    better = true;
                } else if (unlocks == bestUnlocks && best != null
                        && victim.getPriority() == best.getPriority()
                        && victim.getSubmittedAtMillis() < bestSubmittingAt) {
                    better = true;
                }
                if (better) {
                    best = victim;
                    bestUnlocks = unlocks;
                    bestSubmittingAt = victim.getSubmittedAtMillis();
                    bestId = victim.getJobId();
                }
            }
        }
        return bestId == null ? null : best;
    }

    private int countUnlocks(Job victim, ResourceSet need, Map<String, ResourceSet> simulatedAvailable) {
        int unlocks = 0;
        for (String nodeId : victim.getAssignedNodeIds()) {
            ResourceSet current = simulatedAvailable.get(nodeId);
            if (current == null) {
                continue;
            }
            if (!ResourceSet.dominates(current, need)
                    && ResourceSet.dominates(ResourceSet.sum(current, victim.getPerNodeResource()), need)) {
                unlocks++;
            }
        }
        return unlocks;
    }

    private record PreemptivePlan(List<String> selectedNodeIds, List<PreemptionRecord> records) {
    }

    // ================= 诊断 =================

    public boolean isLedgerConsistent() {
        lock.lock();
        try {
            return ledger.isConsistent();
        } finally {
            lock.unlock();
        }
    }

    // ================= 响应组装 =================

    private SubmissionReport report(Job job, SubmissionStatus status, ReasonCode code, String message) {
        return new SubmissionReport(job.getJobId(), status, code, message, job.getState());
    }

    private SubmissionReport rejectedReport(String jobId, ReasonCode code, String message) {
        return new SubmissionReport(jobId, SubmissionStatus.REJECTED, code, message, null);
    }
}

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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
     * 提交作业。相同幂等键的并发/重复提交只会产生一个实例：
     * 内容一致 => 返回既有作业（DUPLICATE，不重复占资源）；
     * 内容冲突 => 拒绝（IDEMPOTENCY_KEY_CONFLICT），与普通重复可区分。
     */
    public SubmissionReport submit(String idempotencyKey, String user, int priority,
                                   int nodeCount, ResourceSet perNode) {
        return submit(idempotencyKey, user, priority, nodeCount, perNode, List.of());
    }

    /**
     * 提交作业（可声明前置依赖）。相同幂等键的并发/重复提交只会产生一个实例：
     * 内容一致 => 返回既有作业（DUPLICATE，不重复占资源、不重复挂依赖）；
     * 内容冲突 => 拒绝（IDEMPOTENCY_KEY_CONFLICT），与普通重复可区分。
     *
     * 依赖规则：dependsOn 引用的作业必须已存在于系统中，且不得构成直接或间接环，
     * 违反时在提交时拒绝（DEPENDENCY_NOT_FOUND / DEPENDENCY_CYCLE）。
     */
    public SubmissionReport submit(String idempotencyKey, String user, int priority,
                                   int nodeCount, ResourceSet perNode, List<String> dependsOn) {
        // 非法请求在锁前即可判定。
        validateRequest(idempotencyKey, user, nodeCount, perNode);
        List<String> deps = normalizeDependsOn(dependsOn);

        lock.lock();
        try {
            String existingId = idempotencyIndex.get(idempotencyKey);
            if (existingId != null) {
                Job existing = jobsById.get(existingId);
                if (sameContent(existing, user, priority, nodeCount, perNode, deps)) {
                    log.info("submit duplicate idempotencyKey={} jobId={} state={}（不重复占用资源）",
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

            String jobId = "job-" + (jobSequence.get() + 1);
            ReasonCode depError = validateDependencies(jobId, deps);
            if (depError != ReasonCode.NONE) {
                log.warn("submit rejected idempotencyKey={} reason={} dependsOn={}",
                        idempotencyKey, depError, deps);
                return rejectedReport(null, depError, depError == ReasonCode.DEPENDENCY_CYCLE
                        ? "dependency cycle detected: dependsOn=" + deps
                                + " would make job " + jobId + " depend on itself directly or indirectly"
                        : "dependency not found: dependsOn=" + deps
                                + " references job(s) that do not exist in the system");
            }
            jobSequence.incrementAndGet();

            Job job = new Job(jobId, idempotencyKey, user, priority, nodeCount, perNode,
                    System.currentTimeMillis(), deps);
            jobsById.put(jobId, job);
            idempotencyIndex.put(idempotencyKey, jobId);
            log.info("submit accepted jobId={} idempotencyKey={} user={} priority={} nodes={} perNode={} dependsOn={}",
                    jobId, idempotencyKey, user, priority, nodeCount, perNode, deps);

            ReasonCode code = scheduleOne(job);
            return report(job, SubmissionStatus.ACCEPTED, code,
                    job.getState() == JobState.RUNNING
                            ? "job scheduled immediately on " + job.getAssignedNodeIds()
                            : job.getLastReason());
        } finally {
            lock.unlock();
        }
    }

    private void validateRequest(String idempotencyKey, String user, int nodeCount, ResourceSet perNode) {
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
    }

    /** 归一化前置依赖列表：去重（保序），空/空白引用视为非法请求。 */
    private List<String> normalizeDependsOn(List<String> dependsOn) {
        if (dependsOn == null || dependsOn.isEmpty()) {
            return List.of();
        }
        List<String> deps = new ArrayList<>();
        for (String dep : dependsOn) {
            if (dep == null || dep.isBlank()) {
                throw new ScheduleException(ReasonCode.INVALID_REQUEST,
                        "dependsOn entries must be non-blank job ids");
            }
            if (!deps.contains(dep)) {
                deps.add(dep);
            }
        }
        return deps;
    }

    /**
     * 提交时依赖校验（锁内调用）：
     * 引用的作业必须已存在（DEPENDENCY_NOT_FOUND）；
     * 不得构成直接或间接环（DEPENDENCY_CYCLE，含自环）。
     */
    private ReasonCode validateDependencies(String newJobId, List<String> deps) {
        for (String dep : deps) {
            if (dep.equals(newJobId)) {
                return ReasonCode.DEPENDENCY_CYCLE;
            }
            if (!jobsById.containsKey(dep)) {
                return ReasonCode.DEPENDENCY_NOT_FOUND;
            }
        }
        Map<String, List<String>> adjacency = new LinkedHashMap<>();
        for (Job j : jobsById.values()) {
            adjacency.put(j.getJobId(), j.getDependsOn());
        }
        return wouldCreateCycle(adjacency, newJobId, deps)
                ? ReasonCode.DEPENDENCY_CYCLE : ReasonCode.NONE;
    }

    /**
     * 通用成环检测（纯函数，便于直接单测间接环）：在既有依赖图 adjacency 上
     * 加入 newNode -> newEdges 的边后，若从 newEdges 出发能回到 newNode 则成环。
     */
    public static boolean wouldCreateCycle(Map<String, List<String>> adjacency,
                                           String newNode, List<String> newEdges) {
        Map<String, List<String>> graph = new LinkedHashMap<>(adjacency);
        graph.put(newNode, newEdges);
        // 从 newNode 出发沿依赖边 DFS，能回到 newNode 即环（直接自环或间接环）。
        List<String> stack = new ArrayList<>(newEdges);
        java.util.Set<String> visited = new java.util.HashSet<>();
        while (!stack.isEmpty()) {
            String cur = stack.remove(stack.size() - 1);
            if (cur.equals(newNode)) {
                return true;
            }
            if (visited.add(cur)) {
                stack.addAll(graph.getOrDefault(cur, List.of()));
            }
        }
        return false;
    }

    private boolean sameContent(Job existing, String user, int priority, int nodeCount,
                                ResourceSet perNode, List<String> dependsOn) {
        return existing.getUser().equals(user)
                && existing.getPriority() == priority
                && existing.getNodeCount() == nodeCount
                && existing.getPerNodeResource().asMap().equals(perNode.asMap())
                && existing.getDependsOn().equals(dependsOn);
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

    /** 正常完成：只有运行中作业可完成，完整释放资源后扫描队列。 */
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
            propagateDependencyOutcome(job);
            runSchedulePass();
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** 取消：排队作业直接取消（无资源）；运行中作业取消并释放资源。 */
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
                log.info("cancel running jobId={} 已释放全部资源", jobId);
                propagateDependencyOutcome(job);
                runSchedulePass();
            } else {
                job.setState(JobState.CANCELLED);
                job.setLastReason("cancelled by user while " + state.name().toLowerCase());
                log.info("cancel {} jobId={}", state.name().toLowerCase(), jobId);
                propagateDependencyOutcome(job);
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
        ReasonCode dep = evaluateDependencies(job);
        if (dep != ReasonCode.NONE) {
            return dep;
        }
        return tryStart(job);
    }

    /**
     * 依赖评估（锁内调用）：等待前置的作业不占资源、不预留、不计配额、不参与调度扫描。
     * - 任一前置已处于失败终态（CANCELLED/PREEMPTED/DEPENDENCY_FAILED）
     *   => 本作业落入 DEPENDENCY_FAILED 终态，并沿依赖链向下传递；
     * - 否则仍有前置未成功完成 => 进入/保持 WAITING，原因中列出仍在等待的作业；
     * - 全部前置 COMPLETED => 返回 NONE（WAITING 中的作业被解锁为 QUEUED，等待本轮调度）。
     */
    private ReasonCode evaluateDependencies(Job job) {
        if (job.getDependsOn().isEmpty()) {
            return ReasonCode.NONE;
        }
        List<String> pending = new ArrayList<>();
        for (String depId : job.getDependsOn()) {
            Job dep = jobsById.get(depId);
            if (dep == null) {
                continue; // 提交时已校验存在；防御性跳过
            }
            JobState s = dep.getState();
            if (s == JobState.CANCELLED || s == JobState.PREEMPTED || s == JobState.DEPENDENCY_FAILED) {
                failDependent(job, dep);
                return ReasonCode.DEPENDENCY_FAILED;
            }
            if (s != JobState.COMPLETED) {
                pending.add(depId);
            }
        }
        if (!pending.isEmpty()) {
            job.setState(JobState.WAITING);
            job.setLastReason("[WAITING_ON_DEPENDENCIES] waiting on predecessor jobs: " + pending);
            log.info("stay-waiting jobId={} waitingOn={}（不占资源/不计配额/不参与调度）",
                    job.getJobId(), pending);
            return ReasonCode.WAITING_ON_DEPENDENCIES;
        }
        if (job.getState() == JobState.WAITING) {
            job.setState(JobState.QUEUED);
            job.setLastReason(null);
            log.info("dependency-unlocked jobId={} 全部前置已成功完成，进入调度队列", job.getJobId());
        }
        return ReasonCode.NONE;
    }

    /** 前置失败终态沿依赖链向下传递：后继落入 DEPENDENCY_FAILED 并继续级联。 */
    private void failDependent(Job dependent, Job failedCause) {
        dependent.setState(JobState.DEPENDENCY_FAILED);
        dependent.setLastReason("[DEPENDENCY_FAILED] predecessor " + failedCause.getJobId()
                + " finished as " + failedCause.getState()
                + " (not COMPLETED); dependent can never be scheduled");
        log.info("dependency-failed jobId={} cause={} causeState={}",
                dependent.getJobId(), failedCause.getJobId(), failedCause.getState());
        propagateDependencyOutcome(dependent);
    }

    /**
     * 前置作业到达终态后，自动重新评估所有等待它的后继（锁内调用）：
     * 成功 => 解锁重新评估；失败 => 后继落入 DEPENDENCY_FAILED 并级联。
     */
    private void propagateDependencyOutcome(Job finished) {
        boolean succeeded = finished.getState() == JobState.COMPLETED;
        for (Job dependent : jobsById.values()) {
            if (dependent.getState() != JobState.WAITING) {
                continue;
            }
            if (!dependent.getDependsOn().contains(finished.getJobId())) {
                continue;
            }
            if (succeeded) {
                evaluateDependencies(dependent);
            } else {
                failDependent(dependent, finished);
            }
        }
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
            propagateDependencyOutcome(victim);
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

    /**
     * best-fit：剩余量相对需求的富余分越小越贴合，优先选用；
     * 同分按节点 id 字典序，保证同一批作业重复调度结论一致。
     */
    private Comparator<ClusterNode> bestFitComparator(ResourceSet need) {
        return Comparator
                .comparingLong((ClusterNode n) -> slackScore(n.available(), need))
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
        // 与普通放置同一套 best-fit 规则：富余分小者优先，同分按节点 id，保证结论可复现。
        ready.sort(Comparator.comparingLong((String id) -> slackScore(simulatedAvailable.get(id), need))
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

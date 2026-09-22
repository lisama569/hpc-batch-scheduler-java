package com.github.lisama569.hpcbatchschedulerjava.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 批处理作业：一组必须成组调度的任务、生命周期状态，以及可解释的判定日志。
 * 可变状态仅在 {@link com.github.lisama569.hpcbatchschedulerjava.service.JobService}
 * 的统一锁内变更。
 */
public final class Job {

    private final String id;
    private final String idempotencyKey;
    private final String owner;
    private final int priority;
    private final List<TaskSpec> tasks;
    private final long submittedAtNanos;

    private JobState state = JobState.QUEUED;
    private Map<Integer, String> placement = Collections.emptyMap();
    private ReasonCode lastReason = null;
    private final List<String> decisionLog = new ArrayList<>();
    private int preemptCount = 0;
    private String lastPreemptedByJobId = null;

    public Job(String id, String idempotencyKey, String owner, int priority, List<TaskSpec> tasks) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.owner = owner;
        this.priority = priority;
        this.tasks = List.copyOf(tasks);
        this.submittedAtNanos = System.nanoTime();
    }

    public String id() {
        return id;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }

    public String owner() {
        return owner;
    }

    public int priority() {
        return priority;
    }

    public List<TaskSpec> tasks() {
        return tasks;
    }

    public JobState state() {
        return state;
    }

    public void setState(JobState state) {
        this.state = state;
    }

    public Map<Integer, String> placement() {
        return placement;
    }

    public long submittedAtNanos() {
        return submittedAtNanos;
    }

    public ReasonCode lastReason() {
        return lastReason;
    }

    /** 记录最近一次调度/准入判定：原因码 + 逐行判定依据，并同步打印到测试日志。 */
    public void recordDecision(ReasonCode code, List<String> log) {
        this.lastReason = code;
        this.decisionLog.clear();
        this.decisionLog.addAll(log);
    }

    public List<String> decisionLog() {
        return Collections.unmodifiableList(decisionLog);
    }

    public int preemptCount() {
        return preemptCount;
    }

    public String lastPreemptedByJobId() {
        return lastPreemptedByJobId;
    }

    /** 被抢占：清失落点、回到排队队列，累计次数与判定依据。 */
    public void markPreempted(String byJobId, List<String> log) {
        this.lastPreemptedByJobId = byJobId;
        this.preemptCount++;
        this.placement = Collections.emptyMap();
        this.state = JobState.QUEUED;
        this.lastReason = null;
        this.decisionLog.clear();
        this.decisionLog.addAll(log);
    }

    public void markPlaced(Map<Integer, String> placement) {
        this.placement = Collections.unmodifiableMap(new LinkedHashMap<>(placement));
        this.state = JobState.RUNNING;
    }
}

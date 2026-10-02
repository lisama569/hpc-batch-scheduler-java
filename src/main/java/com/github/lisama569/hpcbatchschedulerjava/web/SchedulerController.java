package com.github.lisama569.hpcbatchschedulerjava.web;

import com.github.lisama569.hpcbatchschedulerjava.domain.ClusterNode;
import com.github.lisama569.hpcbatchschedulerjava.domain.Job;
import com.github.lisama569.hpcbatchschedulerjava.domain.NodeState;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceType;
import com.github.lisama569.hpcbatchschedulerjava.domain.UserQuota;
import com.github.lisama569.hpcbatchschedulerjava.dto.JobView;
import com.github.lisama569.hpcbatchschedulerjava.dto.NodeRequest;
import com.github.lisama569.hpcbatchschedulerjava.dto.NodeStateRequest;
import com.github.lisama569.hpcbatchschedulerjava.dto.NodeView;
import com.github.lisama569.hpcbatchschedulerjava.dto.QuotaRequest;
import com.github.lisama569.hpcbatchschedulerjava.dto.SubmitJobRequest;
import com.github.lisama569.hpcbatchschedulerjava.dto.SubmitJobResponse;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.ReasonCode;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionReport;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SubmissionStatus;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 调度系统的 HTTP 入口。所有判定都直接来自 SchedulerService，
 * 与进程内入口（直接调用同一 Bean）得到完全一致、可解释的结论。
 */
@RestController
@RequestMapping("/api")
public class SchedulerController {

    private final SchedulerService scheduler;

    public SchedulerController(SchedulerService scheduler) {
        this.scheduler = scheduler;
    }

    // ---------------- 作业 ----------------

    @PostMapping("/jobs")
    public ResponseEntity<SubmitJobResponse> submitJob(@RequestBody SubmitJobRequest request) {
        ResourceSet perNode = ResourceSet.of(
                nz(request.cpu()), nz(request.memoryMb()), nz(request.gpu()));
        int priority = request.priority() == null ? 0 : request.priority();
        int nodeCount = request.nodeCount() == null ? 1 : request.nodeCount();

        SubmissionReport report = scheduler.submit(
                request.idempotencyKey(), request.user(), priority, nodeCount, perNode,
                request.dependsOn() == null ? List.of() : request.dependsOn());

        HttpStatus httpStatus = switch (report.status()) {
            case ACCEPTED -> HttpStatus.CREATED;
            case DUPLICATE -> HttpStatus.OK;
            case REJECTED -> HttpStatus.UNPROCESSABLE_ENTITY;
        };
        return ResponseEntity.status(httpStatus).body(toResponse(report));
    }

    @GetMapping("/jobs")
    public List<JobView> listJobs() {
        return scheduler.listJobs().stream().map(SchedulerController::toJobView).toList();
    }

    @GetMapping("/jobs/{jobId}")
    public JobView getJob(@PathVariable String jobId) {
        Job job = scheduler.getJob(jobId);
        if (job == null) {
            throw new ApiException(ReasonCode.JOB_NOT_FOUND, "job not found: " + jobId, 404);
        }
        return toJobView(job);
    }

    @PostMapping("/jobs/{jobId}/complete")
    public Map<String, Object> completeJob(@PathVariable String jobId) {
        scheduler.completeJob(jobId);
        return Map.of("jobId", jobId, "action", "complete", "result", "ok");
    }

    @PostMapping("/jobs/{jobId}/cancel")
    public Map<String, Object> cancelJob(@PathVariable String jobId) {
        scheduler.cancelJob(jobId);
        return Map.of("jobId", jobId, "action", "cancel", "result", "ok");
    }

    // ---------------- 节点 ----------------

    @PostMapping("/nodes")
    public Map<String, Object> registerNode(@RequestBody NodeRequest request) {
        ResourceSet capacity = ResourceSet.of(
                nz(request.cpu()), nz(request.memoryMb()), nz(request.gpu()));
        boolean created = scheduler.registerNode(request.nodeId(), capacity);
        if (!created) {
            throw new ApiException(ReasonCode.NODE_NOT_FOUND,
                    "node already exists: " + request.nodeId(), 409);
        }
        return Map.of("nodeId", request.nodeId(), "registered", true);
    }

    @GetMapping("/nodes")
    public List<NodeView> listNodes() {
        return scheduler.listNodes().stream().map(SchedulerController::toNodeView).toList();
    }

    @PostMapping("/nodes/{nodeId}/state")
    public Map<String, Object> setNodeState(@PathVariable String nodeId,
                                            @RequestBody NodeStateRequest request) {
        NodeState state;
        try {
            state = NodeState.valueOf(request.state());
        } catch (RuntimeException ex) {
            throw new ApiException(ReasonCode.INVALID_REQUEST,
                    "unknown node state: " + request.state()
                            + " (expected HEALTHY/DRAINING/UNAVAILABLE)", 400);
        }
        boolean ok = scheduler.setNodeState(nodeId, state);
        if (!ok) {
            throw new ApiException(ReasonCode.NODE_NOT_FOUND, "node not found: " + nodeId, 404);
        }
        return Map.of("nodeId", nodeId, "state", state.name());
    }

    // ---------------- 配额与调度 ----------------

    @PostMapping("/quotas")
    public Map<String, Object> setQuota(@RequestBody QuotaRequest request) {
        UserQuota quota = new UserQuota(
                request.user(),
                request.maxRunningJobs() == null ? Integer.MAX_VALUE : request.maxRunningJobs(),
                request.maxCpu() == null ? Long.MAX_VALUE : request.maxCpu(),
                request.maxMemoryMb() == null ? Long.MAX_VALUE : request.maxMemoryMb());
        scheduler.setQuota(quota);
        return Map.of("user", request.user(), "configured", true);
    }

    @PostMapping("/scheduler/tick")
    public Map<String, Object> tick() {
        int started = scheduler.scheduleTick();
        return Map.of("started", started);
    }

    // ---------------- 视图映射 ----------------

    static SubmitJobResponse toResponse(SubmissionReport report) {
        return new SubmitJobResponse(
                report.jobId(),
                report.status().name(),
                report.reasonCode().name(),
                report.message(),
                report.jobState() == null ? null : report.jobState().name());
    }

    static JobView toJobView(Job job) {
        ResourceSet r = job.getPerNodeResource();
        return new JobView(
                job.getJobId(),
                job.getIdempotencyKey(),
                job.getUser(),
                job.getPriority(),
                job.getNodeCount(),
                r.get(ResourceType.CPU),
                r.get(ResourceType.MEMORY_MB),
                r.get(ResourceType.GPU),
                job.getState().name(),
                job.getAssignedNodeIds(),
                job.getDependsOnJobIds(),
                job.getLastReason(),
                job.getPreemptedByJobId());
    }

    static NodeView toNodeView(ClusterNode node) {
        return new NodeView(node.getNodeId(),
                stringify(node.getCapacity()),
                stringify(node.getUsed()),
                node.getState().name(),
                List.copyOf(node.getRunningJobIds()));
    }

    private static Map<String, Long> stringify(ResourceSet set) {
        Map<String, Long> m = new LinkedHashMap<>();
        for (ResourceType type : ResourceType.values()) {
            m.put(type.name(), set.get(type));
        }
        return m;
    }

    private static long nz(Long value) {
        return value == null ? 0L : value;
    }
}

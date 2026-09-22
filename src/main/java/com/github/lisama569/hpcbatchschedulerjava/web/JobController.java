package com.github.lisama569.hpcbatchschedulerjava.web;

import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.model.TaskSpec;
import com.github.lisama569.hpcbatchschedulerjava.service.InvalidRequestException;
import com.github.lisama569.hpcbatchschedulerjava.service.JobService;
import com.github.lisama569.hpcbatchschedulerjava.web.dto.JobResponse;
import com.github.lisama569.hpcbatchschedulerjava.web.dto.SubmitJobRequest;
import com.github.lisama569.hpcbatchschedulerjava.web.dto.TaskRequest;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * 作业相关 REST 入口。控制器只做 DTO 转换，全部判定由 {@link JobService} 完成，
 * 因此 HTTP 入口与进程内调用得到一致的排队/拒绝/抢占结论。
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    public JobResponse submit(@RequestBody SubmitJobRequest request) {
        if (request == null) {
            throw new InvalidRequestException("请求体不能为空");
        }
        int priority = request.priority() == null ? 0 : request.priority();
        List<TaskSpec> tasks = new ArrayList<>();
        if (request.tasks() != null) {
            for (TaskRequest t : request.tasks()) {
                if (t == null) {
                    throw new InvalidRequestException("tasks 中不能包含空元素");
                }
                tasks.add(new TaskSpec(t.name(), Resources.of(
                        nz(t.cpu()), nz(t.memoryMb()), nz(t.gpu()))));
            }
        }
        return JobResponse.from(jobService.submit(
                request.idempotencyKey(), request.owner(), priority, tasks));
    }

    private static long nz(Long value) {
        return value == null ? 0L : value;
    }

    @GetMapping("/{jobId}")
    public JobResponse get(@PathVariable String jobId) {
        return jobService.findJob(jobId)
                .map(JobResponse::from)
                .orElseThrow(() -> new InvalidRequestException("作业不存在: " + jobId));
    }

    @GetMapping
    public List<JobResponse> list() {
        return jobService.listJobs().stream().map(JobResponse::from).toList();
    }

    @PostMapping("/{jobId}/complete")
    public JobResponse complete(@PathVariable String jobId) {
        return jobService.complete(jobId)
                .map(JobResponse::from)
                .orElseThrow(() -> new InvalidRequestException("作业不存在: " + jobId));
    }

    @PostMapping("/schedule")
    public List<JobResponse> schedule() {
        jobService.schedulePending();
        return jobService.listJobs().stream().map(JobResponse::from).toList();
    }
}

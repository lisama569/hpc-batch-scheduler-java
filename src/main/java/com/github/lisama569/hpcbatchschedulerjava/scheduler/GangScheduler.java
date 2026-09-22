package com.github.lisama569.hpcbatchschedulerjava.scheduler;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Job;
import com.github.lisama569.hpcbatchschedulerjava.model.ResourceType;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.model.SchedulePlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 成组调度器（gang scheduling）。
 *
 * <p>一个作业包含多个任务，必须在同一时刻为所有任务找到落点才能启动：
 * 先用账本在锁内试算 first-fit 落点（不修改任何状态），再原子提交；
 * 试算失败则作业保持排队，并返回逐任务、逐资源的缺口依据，绝不部分分配。
 */
public class GangScheduler {

    private static final Logger log = LoggerFactory.getLogger(GangScheduler.class);

    private final ResourceLedger ledger;

    public GangScheduler(ResourceLedger ledger) {
        this.ledger = ledger;
    }

    /** 试算作业能否成组落地。结果不可行时 reasons 给出判定依据。 */
    public SchedulePlan evaluate(Job job) {
        List<String> reasons = new ArrayList<>();
        reasons.add("成组试算作业 " + job.id() + " owner=" + job.owner()
                + " priority=" + job.priority() + " 任务数=" + job.tasks().size()
                + " 总需求=" + totalRequirement(job).describe());
        Map<Integer, String> placement = ledger.planPlacement(job.tasks(), reasons);
        boolean feasible = placement != null;
        if (feasible) {
            reasons.add("全部 " + job.tasks().size() + " 个任务可同时满足，落点=" + placement);
            log.info("成组试算: 作业={} 可行 落点={}", job.id(), placement);
        } else {
            reasons.add("无法同时满足全部任务，作业保持 QUEUED；集群级缺口=" + clusterShortfall(job));
            log.info("成组试算: 作业={} 不可行 依据={}", job.id(), reasons);
        }
        return new SchedulePlan(feasible, feasible ? placement : Map.of(), List.copyOf(reasons),
                feasible ? Map.of() : clusterShortfall(job));
    }

    /** 原子提交试算落点；提交瞬间再由账本做一次状态/容量复核，杜绝并发下的部分分配。 */
    public boolean commit(Job job, SchedulePlan plan, List<String> logLines) {
        if (!plan.feasible()) {
            return false;
        }
        boolean committed = ledger.commitAtomic(job.tasks(), plan.placement());
        if (committed) {
            job.markPlaced(plan.placement());
            logLines.add("原子提交成功，作业 " + job.id() + " 进入 RUNNING，落点=" + plan.placement());
            log.info("成组提交: 作业={} RUNNING 落点={}", job.id(), plan.placement());
        } else {
            logLines.add("提交瞬间复核失败（节点状态或容量已变化），作业 " + job.id() + " 保持 QUEUED，未发生部分分配");
        }
        return committed;
    }

    private Resources totalRequirement(Job job) {
        Resources total = Resources.empty();
        for (var task : job.tasks()) {
            total = total.plus(task.requirement());
        }
        return total;
    }

    /** 汇总所有 READY 节点空闲量相对作业总需求的集群级缺口（用于可解释性）。 */
    private Map<ResourceType, Long> clusterShortfall(Job job) {
        Resources totalNeeded = totalRequirement(job);
        Map<ResourceType, Long> shortfall = new LinkedHashMap<>();
        ledger.withLock(() -> {
            Resources available = Resources.empty();
            for (var node : ledger.snapshotNodes()) {
                if (node.acceptsNewWork()) {
                    available = available.plus(node.available());
                }
            }
            for (ResourceType type : ResourceType.values()) {
                long gap = totalNeeded.get(type) - available.get(type);
                if (gap > 0) {
                    shortfall.put(type, gap);
                }
            }
            return null;
        });
        return shortfall;
    }
}

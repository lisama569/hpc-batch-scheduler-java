package com.github.lisama569.hpcbatchschedulerjava.scheduler;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Job;
import com.github.lisama569.hpcbatchschedulerjava.model.PreemptionPlan;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.model.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 抢占规划器。
 *
 * <p>规则与可解释性：
 * <ul>
 *   <li>只允许抢占<strong>严格更低优先级</strong>的在跑作业；同级或更高优先级绝不抢占；</li>
 *   <li>按“优先级最低、同优先级最晚提交”的顺序挑选受害者；</li>
 *   <li>只有当抢占某集合后，目标作业的<strong>全部</strong>任务都能成组落地才抢占，
 *       否则一个都不抢，避免白抢和资源回收后目标仍然排队；</li>
 *   <li>规划全程只读账本（虚拟释放试算），真正的资源回收由提交阶段在锁内原子完成。</li>
 * </ul>
 */
public class PreemptionPlanner {

    private static final Logger log = LoggerFactory.getLogger(PreemptionPlanner.class);

    private final ResourceLedger ledger;

    public PreemptionPlanner(ResourceLedger ledger) {
        this.ledger = ledger;
    }

    /**
     * @param job          申请资源的目标作业
     * @param runningJobs  当前所有在跑作业
     */
    public PreemptionPlan evaluate(Job job, List<Job> runningJobs) {
        List<String> reasons = new ArrayList<>();
        reasons.add("抢占试算作业 " + job.id() + " priority=" + job.priority());

        List<Job> protectedJobs = runningJobs.stream()
                .filter(other -> other.priority() >= job.priority())
                .sorted(Comparator.comparing(Job::id))
                .toList();
        if (!protectedJobs.isEmpty()) {
            reasons.add("受保护（不可抢占）作业: " + protectedJobs.stream()
                    .map(j -> j.id() + "(priority=" + j.priority() + ")")
                    .toList());
        }

        List<Job> candidates = runningJobs.stream()
                .filter(other -> other.priority() < job.priority())
                .sorted(Comparator.comparingInt(Job::priority)
                        .thenComparing(Job::submittedAtNanos, Comparator.reverseOrder())
                        .thenComparing(Job::id))
                .collect(java.util.stream.Collectors.toCollection(ArrayList::new));

        if (candidates.isEmpty()) {
            reasons.add("不存在严格更低优先级的在跑作业，无法抢占，作业保持 QUEUED");
            log.info("抢占试算: 作业={} 无更低优先级受害者", job.id());
            return new PreemptionPlan(false, List.of(), Map.of(), List.copyOf(reasons));
        }
        reasons.add("候选受害者（按优先级升序）: " + candidates.stream()
                .map(j -> j.id() + "(priority=" + j.priority() + ")")
                .toList());

        Map<String, Resources> releasedExtra = new LinkedHashMap<>();
        List<String> victims = new ArrayList<>();

        // 先看不抢占是否可行（正常情况下调用方已判定不可行，这里防御性处理）
        if (tryPlan(job, releasedExtra, reasons) != null) {
            return new PreemptionPlan(true, List.of(),
                    tryPlan(job, releasedExtra, new ArrayList<>()), List.copyOf(reasons));
        }

        for (Job victim : candidates) {
            victims.add(victim.id());
            addReleased(victim, releasedExtra);
            reasons.add("尝试抢占 " + victim.id() + "(priority=" + victim.priority()
                    + ")，累计释放按节点: " + describeReleased(releasedExtra));
            Map<Integer, String> placement = tryPlan(job, releasedExtra, reasons);
            if (placement != null) {
                reasons.add("抢占 " + victims + " 后全部任务可成组落地，落点=" + placement);
                log.info("抢占试算: 作业={} 可行 受害者={} 落点={}", job.id(), victims, placement);
                return new PreemptionPlan(true, List.copyOf(victims), placement, List.copyOf(reasons));
            }
        }

        reasons.add("即使抢占全部 " + victims.size() + " 个更低优先级作业仍无法让作业成组落地，"
                + "因此不抢占任何作业，作业保持 QUEUED（无资源回收发生）");
        log.info("抢占试算: 作业={} 即使抢占全部低优先级作业仍不可行，放弃抢占", job.id());
        return new PreemptionPlan(false, List.of(), Map.of(), List.copyOf(reasons));
    }

    private Map<Integer, String> tryPlan(Job job, Map<String, Resources> releasedExtra, List<String> reasons) {
        return ledger.planPlacementWithRelease(job.tasks(), releasedExtra, reasons);
    }

    /** 按作业落点汇总其在各节点占用的资源（抢占后这些资源将被完整回收）。 */
    private void addReleased(Job victim, Map<String, Resources> releasedExtra) {
        List<TaskSpec> tasks = victim.tasks();
        for (Map.Entry<Integer, String> entry : victim.placement().entrySet()) {
            Resources req = tasks.get(entry.getKey()).requirement();
            releasedExtra.merge(entry.getValue(), req, Resources::plus);
        }
    }

    private String describeReleased(Map<String, Resources> releasedExtra) {
        List<String> parts = new ArrayList<>();
        releasedExtra.forEach((node, res) -> parts.add(node + "=" + res.describe()));
        return String.join("; ", parts);
    }
}

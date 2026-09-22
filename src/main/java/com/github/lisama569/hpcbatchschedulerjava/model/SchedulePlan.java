package com.github.lisama569.hpcbatchschedulerjava.model;

import java.util.List;
import java.util.Map;

/** 成组调度的试算结论：要么所有任务都有落点，要么给出逐任务的缺口依据。 */
public record SchedulePlan(
        boolean feasible,
        Map<Integer, String> placement,
        List<String> reasons,
        Map<ResourceType, Long> clusterShortfall
) {
}

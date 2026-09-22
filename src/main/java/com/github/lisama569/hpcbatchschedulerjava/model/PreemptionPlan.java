package com.github.lisama569.hpcbatchschedulerjava.model;

import java.util.List;
import java.util.Map;

/** 抢占试算结论：抢占后必须能让目标作业完整落地，否则不抢占。 */
public record PreemptionPlan(
        boolean feasible,
        List<String> victimJobIds,
        Map<Integer, String> placement,
        List<String> reasons
) {
}

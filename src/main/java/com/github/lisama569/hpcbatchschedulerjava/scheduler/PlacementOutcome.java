package com.github.lisama569.hpcbatchschedulerjava.scheduler;

import java.util.List;
import java.util.Map;

/**
 * 一次成组放置尝试的判定结论。
 * success=false 时 explanation 给出可读依据，nodeShortfalls 给出逐节点缺额。
 */
public record PlacementOutcome(boolean success,
                               List<String> selectedNodeIds,
                               Map<String, Map<String, Long>> nodeShortfalls,
                               String explanation) {

    public static PlacementOutcome ok(List<String> nodeIds, String explanation) {
        return new PlacementOutcome(true, nodeIds, Map.of(), explanation);
    }

    public static PlacementOutcome fail(Map<String, Map<String, Long>> shortfalls, String explanation) {
        return new PlacementOutcome(false, List.of(), shortfalls, explanation);
    }
}

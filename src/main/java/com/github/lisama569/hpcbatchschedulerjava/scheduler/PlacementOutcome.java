package com.github.lisama569.hpcbatchschedulerjava.scheduler;

import java.util.List;
import java.util.Map;

/**
 * 一次成组放置尝试的判定结论。
 * success=false 时 reasonCode 给出结构化排队原因（NONE 表示尚未确定），
 * explanation 给出可读依据，nodeShortfalls 给出逐节点缺额。
 */
public record PlacementOutcome(boolean success,
                               List<String> selectedNodeIds,
                               Map<String, Map<String, Long>> nodeShortfalls,
                               ReasonCode reasonCode,
                               String explanation) {

    public static PlacementOutcome ok(List<String> nodeIds, String explanation) {
        return new PlacementOutcome(true, nodeIds, Map.of(), ReasonCode.NONE, explanation);
    }

    public static PlacementOutcome fail(Map<String, Map<String, Long>> shortfalls,
                                        ReasonCode reasonCode, String explanation) {
        return new PlacementOutcome(false, List.of(), shortfalls, reasonCode, explanation);
    }
}

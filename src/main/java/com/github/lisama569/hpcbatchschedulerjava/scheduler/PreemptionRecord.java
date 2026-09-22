package com.github.lisama569.hpcbatchschedulerjava.scheduler;

import java.util.List;

/** 一次抢占的解释记录：谁抢占了谁、释放了哪些节点、依据是什么。 */
public record PreemptionRecord(String preemptedJobId,
                               int preemptedPriority,
                               String preemptedByJobId,
                               int requesterPriority,
                               List<String> nodesReleased,
                               String basis) {
}

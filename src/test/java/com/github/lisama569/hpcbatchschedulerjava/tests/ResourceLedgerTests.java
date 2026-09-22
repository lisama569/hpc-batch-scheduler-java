package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Node;
import com.github.lisama569.hpcbatchschedulerjava.model.NodeState;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.model.TaskSpec;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** 资源账本：容量守恒、超额拒绝、排空节点不接收分配。 */
class ResourceLedgerTests {

    private static final Logger log = LoggerFactory.getLogger(ResourceLedgerTests.class);

    private ResourceLedger newLedger() {
        ResourceLedger ledger = new ResourceLedger();
        ledger.addNode("n1", Resources.of(4, 8192, 1));
        ledger.addNode("n2", Resources.of(4, 8192, 1));
        return ledger;
    }

    @Test
    void allocatesAndReleasesConservingCapacity() {
        ResourceLedger ledger = newLedger();
        log.info("[输入] 分配 cpu=2/mem=4096 到 n1，再释放");
        assertTrue(ledger.tryAllocate("n1", Resources.of(2, 4096, 0)), "容量内应分配成功");
        assertEquals(2, ledger.getNode("n1").used().get(com.github.lisama569.hpcbatchschedulerjava.model.ResourceType.CPU));
        ledger.release("n1", Resources.of(2, 4096, 0));
        log.info("[判定] 释放后 n1 占用={}，期望全 0", ledger.getNode("n1").used().describe());
        assertEquals(0, ledger.getNode("n1").used().get(com.github.lisama569.hpcbatchschedulerjava.model.ResourceType.CPU));
        assertTrue(ledger.verifyInvariants().isEmpty(), "容量守恒: " + ledger.verifyInvariants());
    }

    @Test
    void rejectsOverCommitAndDrainingNodes() {
        ResourceLedger ledger = newLedger();
        log.info("[输入] 超额分配 cpu=8（容量 4）");
        assertFalse(ledger.tryAllocate("n1", Resources.of(8, 0, 0)), "超额必须拒绝");
        ledger.setState("n1", NodeState.DRAINING);
        log.info("[输入] n1 置为 DRAINING 后再申请 cpu=1");
        assertFalse(ledger.tryAllocate("n1", Resources.of(1, 0, 0)), "排空节点不接收新作业");
        assertTrue(ledger.verifyInvariants().isEmpty());
    }

    @Test
    void atomicCommitRollsBackPartialAllocation() {
        ResourceLedger ledger = newLedger();
        ledger.tryAllocate("n1", Resources.of(3, 6000, 1));
        List<TaskSpec> tasks = List.of(
                new TaskSpec("t0", Resources.of(2, 1000, 1)),
                new TaskSpec("t1", Resources.of(2, 1000, 1)));
        List<String> reasons = new java.util.ArrayList<>();
        Map<Integer, String> plan = ledger.planPlacement(tasks, reasons);
        log.info("[输入] 成组任务需求 n1 剩余 cpu=1/gpu=0；试算结果={} 依据={}", plan, reasons);
        assertNull(plan, "两台都缺 gpu，成组不可行，不能部分分配");
        Node n1 = ledger.getNode("n1");
        log.info("[判定] n1 占用仍为={}（期望 cpu=3，未被污染）", n1.used().describe());
        assertEquals(3, n1.used().get(com.github.lisama569.hpcbatchschedulerjava.model.ResourceType.CPU));
        assertEquals(1, n1.used().get(com.github.lisama569.hpcbatchschedulerjava.model.ResourceType.GPU));
        assertTrue(ledger.verifyInvariants().isEmpty());
    }
}

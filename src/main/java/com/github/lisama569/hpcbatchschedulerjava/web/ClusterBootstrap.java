package com.github.lisama569.hpcbatchschedulerjava.web;

import com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet;
import com.github.lisama569.hpcbatchschedulerjava.domain.UserQuota;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 可选的演示集群初始化：3 个节点 + 两个示例配额。
 * 通过 scheduler.bootstrap-demo=false 关闭（测试默认关闭，保证环境确定）。
 */
@Component
public class ClusterBootstrap {

    private final SchedulerService scheduler;

    @Value("${scheduler.bootstrap-demo:true}")
    private boolean bootstrapDemo;

    public ClusterBootstrap(SchedulerService scheduler) {
        this.scheduler = scheduler;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void bootstrap() {
        if (!bootstrapDemo) {
            return;
        }
        scheduler.registerNode("node-a", ResourceSet.of(8, 16384, 2));
        scheduler.registerNode("node-b", ResourceSet.of(8, 16384, 2));
        scheduler.registerNode("node-c", ResourceSet.of(4, 8192, 0));
        scheduler.setQuota(new UserQuota("alice", 4, 24, 49152));
        scheduler.setQuota(new UserQuota("bob", 2, 8, 16384));
    }
}

package com.github.lisama569.hpcbatchschedulerjava.config;

import com.github.lisama569.hpcbatchschedulerjava.ledger.ResourceLedger;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import com.github.lisama569.hpcbatchschedulerjava.quota.QuotaManager;
import com.github.lisama569.hpcbatchschedulerjava.service.ClusterService;
import com.github.lisama569.hpcbatchschedulerjava.service.JobService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 装配进程内账本、配额与服务，并初始化一个示例集群（3 节点 + 两个属主配额）。 */
@Configuration
public class ClusterBootstrap {

    @Bean
    public ResourceLedger resourceLedger() {
        return new ResourceLedger();
    }

    @Bean
    public QuotaManager quotaManager() {
        return new QuotaManager();
    }

    @Bean
    public JobService jobService(ResourceLedger ledger, QuotaManager quotaManager) {
        return new JobService(ledger, quotaManager);
    }

    @Bean
    public ClusterService clusterService(ResourceLedger ledger, JobService jobService) {
        return new ClusterService(ledger, jobService, jobService.schedulingLock());
    }

    @Bean
    public CommandLineRunner defaultClusterInitializer(ResourceLedger ledger, QuotaManager quotaManager) {
        return args -> {
            ledger.addNode("node-a", Resources.of(8, 16_384, 2));
            ledger.addNode("node-b", Resources.of(8, 16_384, 2));
            ledger.addNode("node-c", Resources.of(4, 8_192, 0));
            quotaManager.setQuota("team-a", Resources.of(16, 32_768, 4));
            quotaManager.setQuota("team-b", Resources.of(8, 16_384, 2));
        };
    }
}

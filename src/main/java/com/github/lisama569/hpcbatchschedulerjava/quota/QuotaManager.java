package com.github.lisama569.hpcbatchschedulerjava.quota;

import com.github.lisama569.hpcbatchschedulerjava.model.ResourceType;
import com.github.lisama569.hpcbatchschedulerjava.model.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 按属主（用户/租户）管理资源配额。
 *
 * <p>语义：作业总需求必须逐类资源落在该属主配额内才允许“调度运行”。
 * 超额作业不会让进程崩溃，而是带着 {@code QUOTA_EXCEEDED} 依据保持排队；
 * 未显式配置配额的属主使用默认无限配额。
 */
public class QuotaManager {

    private static final Logger log = LoggerFactory.getLogger(QuotaManager.class);
    private static final long UNLIMITED = Long.MAX_VALUE;

    private final Map<String, Resources> quotas = new LinkedHashMap<>();
    private final Object lock = new Object();

    public QuotaManager() {
    }

    public void setQuota(String owner, Resources quota) {
        synchronized (lock) {
            quotas.put(owner, quota);
            log.info("配额: 属主={} 配额={}", owner, quota.describe());
        }
    }

    public Resources getQuota(String owner) {
        synchronized (lock) {
            return quotas.getOrDefault(owner, Resources.of(UNLIMITED, UNLIMITED, UNLIMITED));
        }
    }

    /**
     * 准入判定：作业总需求逐类不超过配额。
     * 不满足时向 reasons 追加形如 “owner=u1 MEMORY 需求=20480 超过配额=16384” 的依据。
     */
    public boolean admits(String owner, Resources totalRequired, List<String> reasons) {
        synchronized (lock) {
            Resources quota = getQuota(owner);
            boolean admitted = true;
            for (ResourceType type : ResourceType.values()) {
                long need = totalRequired.get(type);
                long limit = quota.get(type);
                if (need > limit) {
                    admitted = false;
                    reasons.add("owner=" + owner + " " + type
                            + " 需求=" + need + " 超过配额=" + (limit == UNLIMITED ? "无限" : String.valueOf(limit)));
                }
            }
            log.info("配额判定: owner={} 总需求={} 配额={} => {}",
                    owner, totalRequired.describe(), quota.describe(), admitted ? "通过" : "超额");
            return admitted;
        }
    }
}

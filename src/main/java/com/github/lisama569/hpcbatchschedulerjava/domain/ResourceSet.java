package com.github.lisama569.hpcbatchschedulerjava.domain;

import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

/** 不可变的多类资源量（如 CPU 核数、内存 MB、GPU 卡数）。 */
public final class ResourceSet {

    public static final ResourceSet ZERO = new ResourceSet(Map.of());

    private final Map<ResourceType, Long> amounts;

    public ResourceSet(Map<ResourceType, Long> amounts) {
        Map<ResourceType, Long> copy = new LinkedHashMap<>();
        amounts.forEach((type, value) -> {
            if (type == null) {
                throw new IllegalArgumentException("resource type must not be null");
            }
            if (value == null || value < 0) {
                throw new IllegalArgumentException("resource amount must be non-negative: " + type);
            }
            if (value > 0) {
                copy.put(type, value);
            }
        });
        this.amounts = Collections.unmodifiableMap(copy);
    }

    public static ResourceSet of(long cpu, long memoryMb, long gpu) {
        Map<ResourceType, Long> m = new LinkedHashMap<>();
        m.put(ResourceType.CPU, cpu);
        m.put(ResourceType.MEMORY_MB, memoryMb);
        m.put(ResourceType.GPU, gpu);
        return new ResourceSet(m);
    }

    public long get(ResourceType type) {
        return amounts.getOrDefault(type, 0L);
    }

    public Map<ResourceType, Long> asMap() {
        return amounts;
    }

    public boolean isEmpty() {
        return amounts.isEmpty();
    }

    /** 各类资源求和。 */
    public static ResourceSet sum(ResourceSet a, ResourceSet b) {
        Map<ResourceType, Long> m = new LinkedHashMap<>();
        for (ResourceType type : ResourceType.values()) {
            long total = a.get(type) + b.get(type);
            if (total > 0) {
                m.put(type, total);
            }
        }
        return new ResourceSet(m);
    }

    /** a - b，若任一类结果为负返回 null（表示不足）。 */
    public static ResourceSet subtractOrNull(ResourceSet a, ResourceSet b) {
        Map<ResourceType, Long> m = new EnumMap<>(ResourceType.class);
        for (ResourceType type : ResourceType.values()) {
            long diff = a.get(type) - b.get(type);
            if (diff < 0) {
                return null;
            }
            if (diff > 0) {
                m.put(type, diff);
            }
        }
        return new ResourceSet(m);
    }

    /** a 是否逐类 >= b。 */
    public static boolean dominates(ResourceSet a, ResourceSet b) {
        for (ResourceType type : ResourceType.values()) {
            if (a.get(type) < b.get(type)) {
                return false;
            }
        }
        return true;
    }

    /** 乘以 n（用于按节点数计算总需求）。 */
    public ResourceSet times(long n) {
        if (n < 0) {
            throw new IllegalArgumentException("multiplier must be non-negative: " + n);
        }
        if (n == 0 || isEmpty()) {
            return ZERO;
        }
        Map<ResourceType, Long> m = new LinkedHashMap<>();
        amounts.forEach((type, value) -> m.put(type, value * n));
        return new ResourceSet(m);
    }

    @Override
    public String toString() {
        return amounts.toString();
    }
}

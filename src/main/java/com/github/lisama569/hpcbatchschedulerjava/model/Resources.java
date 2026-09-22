package com.github.lisama569.hpcbatchschedulerjava.model;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 不可变的多类资源向量（cpu 核数 / memory MB / gpu 卡数）。
 * 任何分量都不允许为负。
 */
public final class Resources {

    private final Map<ResourceType, Long> amounts;

    private Resources(Map<ResourceType, Long> amounts) {
        EnumMap<ResourceType, Long> copy = new EnumMap<>(ResourceType.class);
        for (ResourceType type : ResourceType.values()) {
            long value = amounts.getOrDefault(type, 0L);
            if (value < 0) {
                throw new IllegalArgumentException("resource amount must not be negative: " + type + "=" + value);
            }
            copy.put(type, value);
        }
        this.amounts = Collections.unmodifiableMap(copy);
    }

    public static Resources of(long cpu, long memoryMb, long gpu) {
        EnumMap<ResourceType, Long> map = new EnumMap<>(ResourceType.class);
        map.put(ResourceType.CPU, cpu);
        map.put(ResourceType.MEMORY, memoryMb);
        map.put(ResourceType.GPU, gpu);
        return new Resources(map);
    }

    public static Resources fromMap(Map<ResourceType, Long> amounts) {
        return new Resources(amounts);
    }

    public static Resources empty() {
        return new Resources(new EnumMap<>(ResourceType.class));
    }

    public long get(ResourceType type) {
        return amounts.getOrDefault(type, 0L);
    }

    /** 当前向量是否在每类资源上都不小于 required（成组调度的“同时满足”判定原语）。 */
    public boolean containsAll(Resources required) {
        for (ResourceType type : ResourceType.values()) {
            if (get(type) < required.get(type)) {
                return false;
            }
        }
        return true;
    }

    public Resources plus(Resources other) {
        EnumMap<ResourceType, Long> sum = new EnumMap<>(ResourceType.class);
        for (ResourceType type : ResourceType.values()) {
            sum.put(type, get(type) + other.get(type));
        }
        return new Resources(sum);
    }

    public Resources minus(Resources other) {
        EnumMap<ResourceType, Long> diff = new EnumMap<>(ResourceType.class);
        for (ResourceType type : ResourceType.values()) {
            diff.put(type, get(type) - other.get(type));
        }
        return new Resources(diff);
    }

    public Map<ResourceType, Long> view() {
        return amounts;
    }

    /** 生成形如 cpu=8/mem=16384MB/gpu=2 的可读描述，用于判定日志。 */
    public String describe() {
        return ResourceType.values().length == 0
                ? "none"
                : java.util.Arrays.stream(ResourceType.values())
                        .map(t -> switch (t) {
                            case CPU -> "cpu=" + get(t);
                            case MEMORY -> "mem=" + get(t) + "MB";
                            case GPU -> "gpu=" + get(t);
                        })
                        .collect(Collectors.joining("/"));
    }

    @Override
    public String toString() {
        return "Resources[" + describe() + "]";
    }
}

package com.github.lisama569.hpcbatchschedulerjava.web.dto;

import com.github.lisama569.hpcbatchschedulerjava.model.Node;
import com.github.lisama569.hpcbatchschedulerjava.model.ResourceType;

/** 节点视图：容量、占用、空闲与状态，用于核对容量守恒。 */
public record NodeResponse(
        String id,
        String state,
        long cpuCapacity, long cpuUsed, long cpuAvailable,
        long memoryCapacity, long memoryUsed, long memoryAvailable,
        long gpuCapacity, long gpuUsed, long gpuAvailable
) {
    public static NodeResponse from(Node node) {
        return new NodeResponse(
                node.id(),
                node.state().name(),
                node.capacity().get(ResourceType.CPU),
                node.used().get(ResourceType.CPU),
                node.available().get(ResourceType.CPU),
                node.capacity().get(ResourceType.MEMORY),
                node.used().get(ResourceType.MEMORY),
                node.available().get(ResourceType.MEMORY),
                node.capacity().get(ResourceType.GPU),
                node.used().get(ResourceType.GPU),
                node.available().get(ResourceType.GPU)
        );
    }
}

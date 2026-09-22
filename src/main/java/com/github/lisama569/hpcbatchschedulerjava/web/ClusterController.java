package com.github.lisama569.hpcbatchschedulerjava.web;

import com.github.lisama569.hpcbatchschedulerjava.model.NodeState;
import com.github.lisama569.hpcbatchschedulerjava.service.ClusterService;
import com.github.lisama569.hpcbatchschedulerjava.service.InvalidRequestException;
import com.github.lisama569.hpcbatchschedulerjava.web.dto.NodeResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/** 集群节点运维 REST 入口。 */
@RestController
@RequestMapping("/api/nodes")
public class ClusterController {

    private final ClusterService clusterService;

    public ClusterController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @GetMapping
    public List<NodeResponse> list() {
        return clusterService.listNodes().stream().map(NodeResponse::from).toList();
    }

    @PostMapping("/{nodeId}/state")
    public NodeResponse markState(@PathVariable String nodeId, @RequestParam String state) {
        try {
            return NodeResponse.from(clusterService.markState(nodeId, NodeState.valueOf(state)));
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("非法节点状态: " + state
                    + "，允许值 READY/UNSCHEDULABLE/DRAINING");
        }
    }

    @PostMapping("/{nodeId}/drain")
    public Map<String, Object> drain(@PathVariable String nodeId,
                                     @RequestParam(defaultValue = "false") boolean evictRunning) {
        List<String> evicted = clusterService.drainNode(nodeId, evictRunning);
        return Map.of("nodeId", nodeId, "state", NodeState.DRAINING.name(),
                "evictedJobs", evicted, "invariants", clusterService.verifyInvariants());
    }

    @PostMapping("/{nodeId}/recover")
    public NodeResponse recover(@PathVariable String nodeId) {
        return NodeResponse.from(clusterService.recoverNode(nodeId));
    }

    @GetMapping("/verify")
    public Map<String, Object> verify() {
        List<String> violations = clusterService.verifyInvariants();
        return Map.of("conserved", violations.isEmpty(), "violations", violations);
    }
}

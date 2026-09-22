package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * 端到端：HTTP 入口与服务入口结论一致——非法 400 INVALID_REQUEST、
 * 重复 409 DUPLICATE_SUBMISSION，且重复提交不产生多个实例、资源总量守恒。
 */
@SpringBootTest(classes = com.github.lisama569.hpcbatchschedulerjava.Application.class)
@AutoConfigureMockMvc
class JobApiIntegrationTests {

    private static final Logger log = LoggerFactory.getLogger(JobApiIntegrationTests.class);

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private ObjectMapper objectMapper;

    private String submitJson(String key, String owner, Integer priority, Object tasks) throws Exception {
        return objectMapper.writeValueAsString(Map.of(
                "idempotencyKey", key,
                "owner", owner,
                "priority", priority == null ? 0 : priority,
                "tasks", tasks));
    }

    @Test
    void httpEntryReportsDuplicate409AndInvalid400() throws Exception {
        String key = "api-key-" + System.nanoTime();
        String validBody = submitJson(key, "team-a", 3,
                List.of(Map.of("name", "t0", "cpu", 1, "memoryMb", 256, "gpu", 0)));
        log.info("[输入 HTTP] 首次提交 key={} priority=3", key);
        String firstResponse = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON).content(validBody))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("RUNNING"))
                .andReturn().getResponse().getContentAsString();
        log.info("[判定 HTTP] 首次提交响应={}", firstResponse);

        log.info("[输入 HTTP] 相同幂等键再次提交");
        String dupResponse = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON).content(validBody))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("DUPLICATE_SUBMISSION"))
                .andExpect(jsonPath("$.existingJob.idempotencyKey").value(key))
                .andReturn().getResponse().getContentAsString();
        log.info("[判定 HTTP] 重复提交响应={}（409，携带既有作业）", dupResponse);

        String invalidBody = objectMapper.writeValueAsString(Map.of(
                "idempotencyKey", " ", "owner", "team-a", "priority", 0, "tasks", List.of()));
        log.info("[输入 HTTP] 非法请求：空幂等键 + 空任务");
        String invalidResponse = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON).content(invalidBody))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value("INVALID_REQUEST"))
                .andReturn().getResponse().getContentAsString();
        log.info("[判定 HTTP] 非法请求响应={}（400，原因码与 409 可区分）", invalidResponse);

        // 账本守恒 + 重复提交只存在一个实例
        mockMvc.perform(get("/api/nodes/verify"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.conserved").value(true));
        mockMvc.perform(get("/api/jobs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.idempotencyKey=='" + key + "')]").value(
                        org.hamcrest.Matchers.hasSize(1)));
        log.info("[判定 HTTP] /api/nodes/verify conserved=true，相同幂等键作业仅有一个实例");
    }
}

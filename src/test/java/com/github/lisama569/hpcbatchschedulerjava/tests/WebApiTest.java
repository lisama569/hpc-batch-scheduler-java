package com.github.lisama569.hpcbatchschedulerjava.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.hpcbatchschedulerjava.scheduler.SchedulerService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP 入口验证：REST 与进程内入口共享同一个 SchedulerService Bean，
 * 幂等、非法请求、排空等结论在 HTTP 下同样可区分、可解释。
 */
@SpringBootTest
@AutoConfigureMockMvc
class WebApiTest {

    private static final Logger log = LoggerFactory.getLogger(WebApiTest.class);
    private final ObjectMapper json = new ObjectMapper();

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private SchedulerService scheduler;

    @Test
    void httpSubmitIdempotencyAndInvalidRequestAndDrain() throws Exception {
        // 通过 HTTP 注册节点与配额
        postJson("/api/nodes", """
                {"nodeId":"n1","cpu":4,"memoryMb":8000,"gpu":1}""");
        postJson("/api/nodes", """
                {"nodeId":"n2","cpu":4,"memoryMb":8000,"gpu":1}""");
        postJson("/api/quotas", """
                {"user":"alice","maxRunningJobs":10,"maxCpu":100,"maxMemoryMb":100000}""");
        log.info("[HTTP 输入] 注册 n1/n2 各4C1GPU，配置 alice 配额");

        String body = """
                {"idempotencyKey":"k1","user":"alice","priority":5,"nodeCount":1,
                 "cpu":4,"memoryMb":1000,"gpu":1}""";

        MvcResult first = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated()).andReturn();
        JsonNode firstJson = json.readTree(first.getResponse().getContentAsString());
        String jobId = firstJson.get("jobId").asText();
        log.info("[HTTP 判定] 首次提交 => 201 status={} reason={} jobId={} jobState={}",
                firstJson.get("status").asText(), firstJson.get("reasonCode").asText(),
                jobId, firstJson.get("jobState").asText());
        assertEquals("ACCEPTED", firstJson.get("status").asText());

        // 重复提交：HTTP 200 + DUPLICATE + 同一 jobId，不重复占用
        MvcResult dup = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk()).andReturn();
        JsonNode dupJson = json.readTree(dup.getResponse().getContentAsString());
        log.info("[HTTP 判定] 重复提交 => 200 status={} reason={} jobId={}",
                dupJson.get("status").asText(), dupJson.get("reasonCode").asText(),
                dupJson.get("jobId").asText());
        assertEquals("DUPLICATE", dupJson.get("status").asText());
        assertEquals("DUPLICATE_SUBMISSION", dupJson.get("reasonCode").asText());
        assertEquals(jobId, dupJson.get("jobId").asText());

        // 非法请求：HTTP 400 + reasonCode=INVALID_REQUEST
        MvcResult invalid = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"idempotencyKey":"k2","user":"alice","nodeCount":0,
                                 "cpu":1,"memoryMb":100,"gpu":0}"""))
                .andExpect(status().isBadRequest()).andReturn();
        JsonNode invalidJson = json.readTree(invalid.getResponse().getContentAsString());
        log.info("[HTTP 判定] nodeCount=0 => 400 reasonCode={} message={}",
                invalidJson.get("reasonCode").asText(), invalidJson.get("message").asText());
        assertEquals("INVALID_REQUEST", invalidJson.get("reasonCode").asText());

        // 排空 n2 后，需要 2 节点的成组作业排队；恢复后可调度
        postJson("/api/nodes/n2/state", """
                {"state":"DRAINING"}""");
        MvcResult blocked = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"idempotencyKey":"k3","user":"alice","priority":5,"nodeCount":2,
                                 "cpu":1,"memoryMb":100,"gpu":0}"""))
                .andExpect(status().isCreated()).andReturn();
        JsonNode blockedJson = json.readTree(blocked.getResponse().getContentAsString());
        log.info("[HTTP 判定] n2 排空中提交 2 节点成组作业 => jobState={} reason={}",
                blockedJson.get("jobState").asText(), blockedJson.get("reasonCode").asText());
        assertEquals("QUEUED", blockedJson.get("jobState").asText());

        // 进程内入口直接操作同一核心：完成 n1 上的作业，恢复 n2，tick
        scheduler.completeJob(jobId);
        postJson("/api/nodes/n2/state", """
                {"state":"HEALTHY"}""");
        postJson("/api/scheduler/tick", "{}");
        MvcResult got = mockMvc.perform(get("/api/jobs/" + blockedJson.get("jobId").asText()))
                .andExpect(status().isOk()).andReturn();
        JsonNode gotJson = json.readTree(got.getResponse().getContentAsString());
        log.info("[HTTP 判定] 资源释放+n2 恢复后 k3 state={}", gotJson.get("state").asText());
        assertEquals("RUNNING", gotJson.get("state").asText());
    }

    private void postJson(String path, String body) throws Exception {
        mockMvc.perform(post(path).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is2xxSuccessful());
    }
}

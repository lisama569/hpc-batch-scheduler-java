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

    @Test
    void httpDependencyWaitsAutoUnlocksAndRejectsBadReferences() throws Exception {
        postJson("/api/nodes", """
                {"nodeId":"dep-n1","cpu":8,"memoryMb":8000,"gpu":0}""");
        postJson("/api/quotas", """
                {"user":"carol","maxRunningJobs":10,"maxCpu":100,"maxMemoryMb":100000}""");
        log.info("[HTTP 输入] 注册 dep-n1 8CPU，配置 carol 配额");

        // 前置 A 通过 HTTP 提交并运行
        MvcResult aRes = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON).content("""
                                {"idempotencyKey":"dep-a","user":"carol","priority":5,"nodeCount":1,
                                 "cpu":4,"memoryMb":1000,"gpu":0}"""))
                .andExpect(status().isCreated()).andReturn();
        String aId = json.readTree(aRes.getResponse().getContentAsString()).get("jobId").asText();
        log.info("[HTTP 判定] 前置 A => jobId={}", aId);

        // 后继 B 依赖 A：必须 WAITING，原因里能看到在等谁
        MvcResult bRes = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON).content("""
                                {"idempotencyKey":"dep-b","user":"carol","priority":5,"nodeCount":1,
                                 "cpu":4,"memoryMb":1000,"gpu":0,"dependsOn":["dep-a"]}"""))
                .andExpect(status().isCreated()).andReturn();
        JsonNode bJson = json.readTree(bRes.getResponse().getContentAsString());
        String bId = bJson.get("jobId").asText();
        log.info("[HTTP 判定] B 依赖 A（按幂等键引用）=> state={} reason={} msg={}",
                bJson.get("jobState").asText(), bJson.get("reasonCode").asText(),
                bJson.get("message").asText());
        assertEquals("WAITING", bJson.get("jobState").asText());
        assertEquals("WAITING_ON_DEPENDENCIES", bJson.get("reasonCode").asText());

        // 引用不存在的作业：HTTP 与进程内结论一致（REJECTED + DEPENDENCY_NOT_FOUND => 422）
        MvcResult bad = mockMvc.perform(post("/api/jobs")
                        .contentType(MediaType.APPLICATION_JSON).content("""
                                {"idempotencyKey":"dep-bad","user":"carol","priority":5,"nodeCount":1,
                                 "cpu":1,"memoryMb":100,"gpu":0,"dependsOn":["job-ghost"]}"""))
                .andExpect(status().isUnprocessableEntity()).andReturn();
        JsonNode badJson = json.readTree(bad.getResponse().getContentAsString());
        log.info("[HTTP 判定] 依赖不存在 => 422 reason={}", badJson.get("reasonCode").asText());
        assertEquals("DEPENDENCY_NOT_FOUND", badJson.get("reasonCode").asText());

        // 通过 HTTP 完成 A：B 必须被自动解锁并直接运行（无需手动 tick）
        postJson("/api/jobs/" + aId + "/complete", "{}");
        MvcResult got = mockMvc.perform(get("/api/jobs/" + bId))
                .andExpect(status().isOk()).andReturn();
        JsonNode gotJson = json.readTree(got.getResponse().getContentAsString());
        log.info("[HTTP 判定] A 完成后 B => state={} dependsOn={}",
                gotJson.get("state").asText(), gotJson.get("dependsOnJobIds").toString());
        assertEquals("RUNNING", gotJson.get("state").asText());
        assertEquals(aId, gotJson.get("dependsOnJobIds").get(0).asText());

        // 通过 HTTP 取消运行中的 B：等 B 的 C（进程内提交）必须落 DEPENDENCY_FAILED，两入口结论一致
        scheduler.submit("dep-c", "carol", 5, 1,
                com.github.lisama569.hpcbatchschedulerjava.domain.ResourceSet.of(1, 100, 0),
                java.util.List.of(bId));
        postJson("/api/jobs/" + bId + "/cancel", "{}");
        var cJob = scheduler.listJobs().stream()
                .filter(j -> j.getIdempotencyKey().equals("dep-c")).findFirst().orElseThrow();
        log.info("[HTTP 判定] HTTP 取消 B 后，进程内提交的 C => state={} reason={}",
                cJob.getState(), cJob.getLastReason());
        assertEquals(com.github.lisama569.hpcbatchschedulerjava.domain.JobState.DEPENDENCY_FAILED,
                cJob.getState());
    }

    private void postJson(String path, String body) throws Exception {
        mockMvc.perform(post(path).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is2xxSuccessful());
    }
}

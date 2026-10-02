# HPC 本地批处理作业调度器

在 Spring Boot 工程内实现的一套**进程内**批处理作业调度与资源分配能力：用一个带锁的资源账本
模拟集群节点，使「HTTP 入口」与「进程内直接调用」在并发提交、成组调度、配额与抢占上得到
**完全一致、可解释**的结论。

## 1. 模块结构

```
domain/      领域模型：ResourceSet(多类资源) / ClusterNode(节点账本) / Job / UserQuota / 枚举
scheduler/   调度核心：SchedulerService(入口) + ResourceLedger(账本) + 原因码/结论记录
web/         HTTP 入口：SchedulerController、全局异常映射、可选演示集群引导
dto/         请求/响应视图
```

- `SchedulerService` 是唯一的判定核心，内部用一把公平 `ReentrantLock` 串行化所有变更，
  因此多线程并发提交、HTTP 并发请求下结论确定，无竞态、无超分。
- `ResourceLedger` 只在锁内被调用，自身不再加锁；节点占用任何时候都满足 `used <= capacity`。

## 2. 作业模型与提交幂等

- 作业请求包含：`idempotencyKey`、`user`、`priority`（越大越高）、`nodeCount`、
  每节点资源 `cpu / memoryMb / gpu`。
- **幂等提交**：相同幂等键只产生一个作业实例。
  - 键相同且请求内容一致：返回 `status=DUPLICATE, reasonCode=DUPLICATE_SUBMISSION`，
    复用既有作业，**不会**第二次占用资源（HTTP `200`）。
  - 键相同但内容不同：拒绝并返回 `IDEMPOTENCY_KEY_CONFLICT`（HTTP `400`），与普通重复可区分。
- **非法请求**（空幂等键/空用户/`nodeCount<=0`/资源全为 0）：抛出 `INVALID_REQUEST`（HTTP `400`），
  不入队、不占资源。
- 用户未配置配额：`UNKNOWN_USER`（HTTP `404`）。

## 3. 成组调度（Gang Scheduling）策略

- 作业需要的所有节点必须**同时**各自满足「每节点资源需求」才能启动，否则保持 `QUEUED`。
- 放置判定顺序：
  1. **配额**（见下节）；
  2. 健康可调度节点中，单机**容量上限** `capacity` 必须能容纳每节点需求
     （`NO_FITTING_NODE` 是硬上限，排空恢复/抢占都改变不了，故提前给出明确原因）；
  3. 当前空闲资源能一次性凑齐 `nodeCount` 个节点 => 立即成组启动；
  4. 否则给出 `INSUFFICIENT_CAPACITY`，响应/日志包含**逐节点缺额**（每类资源还差多少）。
- 节点选择采用 best-fit：富余资源最贴近需求的节点优先，为更大需求保留充裕节点。
- 成组分配是**原子操作**：先校验全部节点，再统一落账；任一节点失败则整体回滚，
  不存在部分分配。资源释放同样按全部分配节点完整回收。
- 触发时机：新提交、作业完成/取消、节点恢复、以及显式 `POST /api/scheduler/tick`；
  一轮内按「优先级降序、同优先级先到先得」扫描排队队列。

## 4. 节点健康与排空

节点三态：`HEALTHY` / `DRAINING` / `UNAVAILABLE`。

- 只有 `HEALTHY` 节点参与新作业放置；排空中与不可用节点**不再接收新作业**。
- 排空/不可用**不会驱逐**已在该节点运行的作业（其资源保留到完成/被抢占）。
- 恢复为 `HEALTHY` 后，下一次调度扫描即可把排队作业放到该节点上。

## 5. 用户配额

按用户统计**当前运行中**作业（排队不计入）：

- `maxRunningJobs`：运行作业数量上限；
- `maxCpu`：运行作业 CPU 总和（`每节点CPU × 节点数` 求和）上限；
- `maxMemoryMb`：运行作业内存总和上限。

超额时作业**保持排队**而非报错终止，原因码分别为 `QUOTA_JOB_LIMIT / QUOTA_CPU_LIMIT /
QUOTA_MEMORY_LIMIT`，消息中带具体上限数值，作为可解释依据；资源释放、配额恢复后可自动启动。

## 6. 抢占规则

当高优先级作业普通放置失败、但存在**严格更低优先级**的运行作业时，调度器进入抢占规划：

1. **只模拟、不落账**：以「真实空闲 + 已计划抢占将释放的资源」推演各节点可用量；
2. 每轮选择一个「抢占后能解锁最多合格节点」的受害者，同解锁数下优先级更低、提交更早者优先；
3. 严格前提：受害者 `priority` 必须**严格小于**请求者——同优先级、更高优先级绝不抢占；
4. 能凑齐 `nodeCount` 个节点才提交：先把所有被抢占作业的资源**完整回收**，再原子成组分配；
5. 抢占后仍无法满足（或单机容量硬上限不满足/无更低优先级作业）=> 不动任何运行作业，
   作业排队，原因为 `PREEMPTION_INSUFFICIENT` / `NO_FITTING_NODE` / `INSUFFICIENT_CAPACITY`。
6. 被抢占作业进入终态 `PREEMPTED`，记录 `preemptedByJobId` 与文字依据（双方优先级、释放节点）。

## 7. 作业依赖编排（先后串联）

提交作业时可通过 `dependsOn` 声明前置作业（**jobId 或提交时的幂等键**均可引用）：

- **等待期（`WAITING` 状态）**：前置未全部 `COMPLETED` 前，后继处于 `WAITING`——
  不占用也不预留节点资源、**不计入用户配额**（配额只统计 `RUNNING`）、不参与调度扫描，
  因此**不会**被误报为 `INSUFFICIENT_CAPACITY`；`lastReason` 形如
  `[WAITING_ON_DEPENDENCIES] waiting for: job-1(RUNNING), job-2(QUEUED)`，一眼可见在等谁。
- **自动解锁**：前置成功完成（`complete`）后，系统在同一事务内自动重新评估全部后继，
  全部前置成功 => 解锁为 `QUEUED` 并由紧随的调度轮次直接启动，**无需手动触发**。
- **失败传播**：前置被**取消**或**被抢占**（或前置自身 `DEPENDENCY_FAILED`）时，
  后继落到可区分终态 **`DEPENDENCY_FAILED`**，`lastReason` 指明是哪个前置、以何种结局终结，
  并沿依赖链**级联**，不会无声悬挂。HTTP 与进程内入口共享同一判定核心，结论一致。
- **提交期校验**（拒绝且原因可区分）：
  - 引用不存在的作业 => `DEPENDENCY_NOT_FOUND`；
  - 依赖成环（含通过自身幂等键的自引用）=> `DEPENDENCY_CYCLE`；
    依赖图是只增的（新作业没有任何后继），间接环结构上不可达，仍有遍历防护兜底。
- **解锁顺序**：一批后继同时被解锁时，按既有统一规则
  「优先级降序、同优先级先提交先调度」启动，稳定且可解释。

### 与既有能力的兼容范围

| 既有能力 | 与依赖的交互 |
| --- | --- |
| 幂等提交 | 依赖列表属于幂等内容：同键同内容（含依赖）=> `DUPLICATE`，不产生第二个实例也**不多挂依赖边**；同键但依赖不同 => `IDEMPOTENCY_KEY_CONFLICT`。 |
| 成组调度/资源账本 | `WAITING` 作业完全在账本之外；解锁后才走原有的原子成组分配，不超分、不出现部分分配。 |
| 配额 | 只统计 `RUNNING`，`WAITING`/`DEPENDENCY_FAILED` 均不计入占用。 |
| 抢占 | 被抢占作业的后继会收到 `DEPENDENCY_FAILED` 传播；抢占判定本身不变。 |
| 节点排空/恢复 | 与依赖正交：排空只影响可调度节点集合，等待中的作业不受影响，恢复后按原规则调度。 |

## 8. 可解释的失败原因（ReasonCode）

| 原因码 | 含义 | HTTP 状态（示例） |
| --- | --- | --- |
| `INVALID_REQUEST` | 参数缺失/越界 | 400 |
| `IDEMPOTENCY_KEY_CONFLICT` | 同幂等键但内容不同 | 400 |
| `DUPLICATE_SUBMISSION` | 同幂等键的重复提交 | 200（提交接口） |
| `UNKNOWN_USER` | 用户未配置配额 | 404 |
| `QUOTA_JOB_LIMIT / QUOTA_CPU_LIMIT / QUOTA_MEMORY_LIMIT` | 配额超额（排队） | 422 |
| `NO_FITTING_NODE` | 单机容量硬上限不满足 | 422 |
| `INSUFFICIENT_CAPACITY` | 临时空闲资源不足（排队） | 422 |
| `PREEMPTION_INSUFFICIENT` | 抢占更低优先级作业后仍不足 | 422 |
| `WAITING_ON_DEPENDENCIES` | 等待前置作业（不占资源/不计配额） | 422（提交响应中携带） |
| `DEPENDENCY_NOT_FOUND` | 依赖引用了不存在的作业 | 404/422 |
| `DEPENDENCY_CYCLE` | 依赖成环（含自引用） | 400/422 |
| `DEPENDENCY_FAILED` | 前置未成功结束（终态原因） | 409 |
| `JOB_NOT_FOUND / NODE_NOT_FOUND / JOB_STATE_CONFLICT` | 资源不存在/状态冲突 | 404/409 |

错误响应体统一为 `{"reasonCode": "...", "message": "..."}`；排队作业的 `lastReason`
字段也保留 `[原因码] 判定依据`，便于追溯。

## 8. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/nodes` | 注册节点 `{nodeId,cpu,memoryMb,gpu}` |
| `GET  /api/nodes` | 查看节点容量、占用、状态与运行作业 |
| `POST /api/nodes/{id}/state` | `{state: HEALTHY/DRAINING/UNAVAILABLE}` |
| `POST /api/quotas` | 设置用户配额 |
| `POST /api/jobs` | 幂等提交作业（可带 `dependsOn: [jobId或幂等键, ...]`） |
| `GET  /api/jobs` / `GET /api/jobs/{id}` | 查询作业（含 lastReason、dependsOnJobIds） |
| `POST /api/jobs/{id}/complete` | 完成作业并释放资源 |
| `POST /api/jobs/{id}/cancel` | 取消排队/运行作业 |
| `POST /api/scheduler/tick` | 显式触发一次调度扫描 |

启动时默认注入演示集群（3 节点 + alice/bob 配额），可用
`scheduler.bootstrap-demo=false` 关闭（测试环境默认关闭）。

快速手工验证：

```bash
mvn spring-boot:run
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","user":"alice","priority":5,"nodeCount":2,
       "cpu":4,"memoryMb":8000,"gpu":1}'
```

依赖编排的本地验证：

```bash
# 1) 提交前置 A（假设返回 jobId=job-1）
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"step-a","user":"alice","priority":5,"nodeCount":1,
       "cpu":2,"memoryMb":1000,"gpu":0}'
# 2) 提交后继 B，dependsOn 可写 jobId 或幂等键 => B 处于 WAITING，不占资源
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"step-b","user":"alice","priority":5,"nodeCount":1,
       "cpu":2,"memoryMb":1000,"gpu":0,"dependsOn":["step-a"]}'
# 3) 完成 A => B 自动解锁并运行，无需手动 tick
curl -s -X POST localhost:8080/api/jobs/job-1/complete
curl -s localhost:8080/api/jobs   # B 应为 RUNNING
# 4) 反向验证：取消前置 => 后继落 DEPENDENCY_FAILED；依赖不存在的作业 => DEPENDENCY_NOT_FOUND
```

## 9. 测试与验证方法

```bash
mvn test
```

覆盖场景（共 29 个用例，全部断言 + 日志打印「输入」与「判定依据」）：

- `IdempotencyTest`：重复提交复用实例不重复占资源、同键内容冲突、非法请求、未知用户；
- `ConcurrentSubmitTest`：32 线程并发同键提交只有 1 个 `ACCEPTED`；
  16 个不同作业并发提交到 8CPU 集群恰好运行 4 个、不超分；
- `GangSchedulingTest`：多类资源跨节点同时满足才启动、不满足保持排队、
  `NO_FITTING_NODE` 与 `INSUFFICIENT_CAPACITY` 可区分、完成后回收再调度；
- `NodeDrainTest`：排空中节点不接新作业且不驱逐既有作业、不可用节点被排除、恢复后排队作业启动；
- `PreemptionQuotaTest`：严格优先级抢占与完整回收、同优先级不被抢占、抢占无意义时不误杀、
  三类配额超额原因；
- `ResourceConservationTest`：填充→跨节点成组抢占→完成→排空/恢复的混合生命周期下，
  从运行作业视角汇总的 CPU/MEM/GPU 与节点账本逐类相等，最终归零，验证无泄漏、无超分；
- `JobDependencyTest`：等待前置不占资源/不计配额/不误报容量不足且原因可见、
  前置成功后自动解锁直接运行、链式逐级解锁、取消/抢占的失败结局向下传播并级联、
  引用不存在与成环（自引用）提交期可区分拒绝、同键重复提交不多挂依赖边、
  一批后继同时解锁按优先级稳定顺序启动、并发提交+并发解锁下账本守恒不超分；
- `WebApiTest`：HTTP 入口与进程内入口共享同一核心 Bean，幂等/非法/排队/恢复结论一致；
  依赖的等待、自动解锁、取消传播与 `DEPENDENCY_NOT_FOUND` 在 HTTP 下结论相同。

调度器在每次启动、排队、抢占、回收和调度轮次结束时都会打印结构化日志
（含优先级、所选节点、逐节点缺额、抢占依据、全局已用资源与 `账本一致=true/false`），
可直接在 surefire 报告或控制台复核判定过程。

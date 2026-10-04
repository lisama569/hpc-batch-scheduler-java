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
  每节点资源 `cpu / memoryMb / gpu`，以及可选的 `dependsOn`（前置作业 id 列表，见第 7 节）。
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
     （`NO_FITTING_NODE` 是硬上限，排空恢复/抢占都改变不了，故提前给出明确原因）。
     注意判定用的是**每节点需求对单机容量**，不是作业总需求：成组作业总量超过单机
     容量是正常的，只要每台节点各自装得下每节点需求即可调度；
  3. 当前**真实剩余量**（`capacity - used`，即扣除已在该节点运行的其他作业）能一次性
     凑齐 `nodeCount` 个节点 => 立即成组启动；只要集群里还有装得下的节点就会落下去，
     不会无故排队；
  4. 否则给出 `INSUFFICIENT_CAPACITY`，响应/日志包含**逐节点缺额**（每类资源还差多少），
     资源释放后的下一轮扫描会自动启动，不会“永远排不上”。
- **落点选择（best-fit）**：在剩余量满足需求的节点中，选「富余分」最小、即剩余资源
  最贴近需求的节点——小作业落到刚好装得下的节点上，把宽裕的大节点留给后面更大的
  作业，避免小作业挤占大节点导致大作业无处可跑。富余分 = 各需求资源类
  `剩余量 - 需求量` 之和；同分按节点 id 字典序，因此同一批作业重复调度结论完全一致，
  可复现、可解释（日志会打印所选节点与判定依据）。
- 成组分配是**原子操作**：先校验全部节点，再统一落账；任一节点失败则整体回滚，
  不存在部分分配。资源释放同样按全部分配节点完整回收。
- 触发时机：新提交、作业完成/取消、节点恢复、以及显式 `POST /api/scheduler/tick`；
  一轮内按「优先级降序、同优先级先到先得」扫描排队队列。所有判定在同一把公平锁内
  串行完成，并发提交、提交与完成交错时每台节点占用都不会超过容量，账本与运行作业
  视角始终对得上。

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

**落点选择与抢占/排空恢复的配合**：

- 抢占规划模拟出的候选节点，与普通放置使用**同一套 best-fit 规则**（富余分小者优先、
  同分按节点 id），因此“直接放得下”与“抢占后放得下”的落点口径一致、结论可复现；
- 被抢占作业的资源先在账本上**完整回收**，再按 best-fit 选出的节点原子成组分配，
  不会出现重复占用；
- 节点排空（`DRAINING`/`UNAVAILABLE`）期间不参与落点；恢复 `HEALTHY` 后的下一轮扫描
  按当时的真实剩余量重新做 best-fit 选择，排队作业自动启动，无需人工干预。

## 7. 作业依赖（dependsOn）

提交作业时可通过 `dependsOn: ["job-1", ...]` 声明前置作业（可多个），把一批本来要按
顺序跑的作业串起来，由系统自己管先后：

- **等待期（`WAITING`）**：前置未全部 `COMPLETED` 前，后继处于独立的 `WAITING` 状态——
  不占用也不预留节点资源、不计入用户配额（配额只统计 `RUNNING`）、不参与调度扫描，
  因此绝不会被误报成容量不足；`lastReason` 为
  `[WAITING_ON_DEPENDENCIES] waiting on predecessor jobs: [...]`，能一眼看出在等哪几个作业。
- **提交时校验**（拒绝不产生任何作业实例，两种原因可区分）：
  - 引用不存在的作业 => `DEPENDENCY_NOT_FOUND`；
  - 直接或间接成环（含自环）=> `DEPENDENCY_CYCLE`。
    通用成环检测见 `SchedulerService.wouldCreateCycle`（纯函数，可单独单测）。
- **成功自动解锁**：前置 `COMPLETED` 时，等待它的后继在同一把锁内被自动重新评估，
  全部前置成功后转为 `QUEUED` 并紧接着进入当轮调度扫描——能跑就直接跑起来，
  无需手动 `tick`。一批后继同时解锁时，启动顺序沿用全局规则
  「优先级降序、同优先级先到先得」，稳定且可解释。
- **失败结局沿链传递**：前置被 `CANCELLED` / `PREEMPTED`（或自身 `DEPENDENCY_FAILED`）时，
  后继落入可区分终态 `DEPENDENCY_FAILED`，`lastReason` 指明是哪个前置、以什么结局结束；
  该结局沿依赖链级联（A 取消 => B 失败 => 等 B 的 C 也失败），不会无声悬挂。
- **与既有能力的兼容范围**：
  - 幂等：`dependsOn` 属于幂等内容——同键同依赖重复提交仍只一个实例、不多挂依赖；
    同键不同依赖按 `IDEMPOTENCY_KEY_CONFLICT` 拒绝。
  - 配额/成组/抢占/节点维护：`WAITING` 不进入任何资源与配额账本，解锁后走与
    普通作业完全相同的配额→成组放置→抢占判定；前置完成、容量释放、并发提交
    同时发生时，所有状态迁移仍在同一把公平锁内串行完成，账本不超分、不部分分配。
  - HTTP 与进程内入口共享同一 `SchedulerService`，依赖结论（等待/解锁/失败传递/拒绝）
    在两种入口下完全一致。

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
| `WAITING_ON_DEPENDENCIES` | 等待前置作业完成（不占资源/不计配额） | 提交返回 201，作业状态 `WAITING` |
| `DEPENDENCY_NOT_FOUND` | 前置依赖引用了不存在的作业 | 400（异常）/ 422（提交报告拒绝） |
| `DEPENDENCY_CYCLE` | 前置依赖直接或间接成环 | 400（异常）/ 422（提交报告拒绝） |
| `DEPENDENCY_FAILED` | 前置未成功结束（取消/抢占/级联失败） | 终态，非错误响应 |
| `JOB_NOT_FOUND / NODE_NOT_FOUND / JOB_STATE_CONFLICT` | 资源不存在/状态冲突 | 404/409 |

错误响应体统一为 `{"reasonCode": "...", "message": "..."}`；排队作业的 `lastReason`
字段也保留 `[原因码] 判定依据`，便于追溯。

## 9. HTTP 接口

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /api/nodes` | 注册节点 `{nodeId,cpu,memoryMb,gpu}` |
| `GET  /api/nodes` | 查看节点容量、占用、状态与运行作业 |
| `POST /api/nodes/{id}/state` | `{state: HEALTHY/DRAINING/UNAVAILABLE}` |
| `POST /api/quotas` | 设置用户配额 |
| `POST /api/jobs` | 幂等提交作业（可选 `dependsOn: ["job-1",...]` 声明前置） |
| `GET  /api/jobs` / `GET /api/jobs/{id}` | 查询作业（含 lastReason、dependsOn） |
| `POST /api/jobs/{id}/complete` | 完成作业并释放资源（自动解锁后继） |
| `POST /api/jobs/{id}/cancel` | 取消排队/等待/运行作业（失败结局沿依赖链传递） |
| `POST /api/scheduler/tick` | 显式触发一次调度扫描 |

启动时默认注入演示集群（3 节点 + alice/bob 配额），可用
`scheduler.bootstrap-demo=false` 关闭（测试环境默认关闭）。

快速手工验证：

```bash
mvn spring-boot:run
# 提交前置作业（立即运行）
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","user":"alice","priority":5,"nodeCount":2,
       "cpu":4,"memoryMb":8000,"gpu":1}'
# 提交后继：等待 k1 对应作业完成，返回 state=WAITING
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k2","user":"alice","priority":5,"nodeCount":1,
       "cpu":2,"memoryMb":4000,"gpu":0,"dependsOn":["job-1"]}'
# 完成前置后，后继自动解锁启动，无需手动 tick
curl -s -X POST localhost:8080/api/jobs/job-1/complete
curl -s localhost:8080/api/jobs/job-2   # state=RUNNING
```

验证落点选择（演示集群 node-a/node-b 为 8C16G2GPU，node-c 为 4C8G0GPU）：

```bash
# 小作业 2C2G：应落到最贴合的 node-c，而不是更大的 node-a/node-b
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"p1","user":"alice","priority":5,"nodeCount":1,
       "cpu":2,"memoryMb":2048,"gpu":0}'
# 用响应中的 jobId 查询：assignedNodeIds 应为 ["node-c"]
curl -s localhost:8080/api/jobs/{jobId}
# 需要 GPU 的大作业：node-c 无 GPU 装不下，落到 node-a/node-b 之一，立即启动
curl -s -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"p2","user":"alice","priority":5,"nodeCount":1,
       "cpu":6,"memoryMb":8192,"gpu":1}'
curl -s localhost:8080/api/nodes        # 各节点占用不超过容量，账目可核对
```

## 10. 测试与验证方法

```bash
mvn test
```

覆盖场景（共 32 个用例，全部断言 + 日志打印「输入」与「判定依据」）：

- `IdempotencyTest`：重复提交复用实例不重复占资源、同键内容冲突、非法请求、未知用户；
- `ConcurrentSubmitTest`：32 线程并发同键提交只有 1 个 `ACCEPTED`；
  16 个不同作业并发提交到 8CPU 集群恰好运行 4 个、不超分；
- `GangSchedulingTest`：多类资源跨节点同时满足才启动、不满足保持排队、
  `NO_FITTING_NODE` 与 `INSUFFICIENT_CAPACITY` 可区分、完成后回收再调度；
- `BestFitPlacementTest`：不同容量节点下小作业落到最贴合节点、随后的大作业立即启动；
  节点被部分占用后按真实剩余量选落点；放不下时 `INSUFFICIENT_CAPACITY`（带逐节点缺额）
  与 `NO_FITTING_NODE` 可区分且释放后自动启动；成组总量超单机容量不被误判为不可行；
  16 线程并发提交 + 提交/完成交错时每节点占用不超容量、账本与运行作业逐台对得上、
  同批作业重复调度落点结论一致；
- `NodeDrainTest`：排空中节点不接新作业且不驱逐既有作业、不可用节点被排除、恢复后排队作业启动；
- `PreemptionQuotaTest`：严格优先级抢占与完整回收、同优先级不被抢占、抢占无意义时不误杀、
  三类配额超额原因；
- `DependencySchedulingTest`：等待前置不占资源/不计配额且原因列出所等作业、
  前置成功后无手动 tick 自动解锁、取消沿依赖链级联为 `DEPENDENCY_FAILED`、
  抢占前置导致后继失败、成环（自环/间接环）与引用不存在以不同原因码拒绝、
  批量解锁顺序确定（优先级降序+先到先得）且恰好占满不超分、
  16 线程并发提交后继并解锁时账本守恒、同键重复提交不多挂依赖；
- `ResourceConservationTest`：填充→跨节点成组抢占→完成→排空/恢复的混合生命周期下，
  从运行作业视角汇总的 CPU/MEM/GPU 与节点账本逐类相等，最终归零，验证无泄漏、无超分；
- `WebApiTest`：HTTP 入口与进程内入口共享同一核心 Bean，幂等/非法/排队/恢复结论一致；
  依赖场景下 HTTP 提交等待、进程内完成前置后 HTTP 侧可见自动解锁，拒绝原因码一致。

调度器在每次启动、排队、抢占、回收和调度轮次结束时都会打印结构化日志
（含优先级、所选节点、逐节点缺额、抢占依据、全局已用资源与 `账本一致=true/false`），
可直接在 surefire 报告或控制台复核判定过程。

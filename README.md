# HPC 本地批处理作业调度器（进程内资源账本）

在 Spring Boot 进程内用一个**并发安全的资源账本**模拟集群节点，提供本地批处理作业的
提交排队、跨节点多类资源的**成组调度（gang scheduling）**、配额准入与**优先级抢占**能力。
REST 入口与进程内服务入口共用同一套带锁的服务逻辑，因此在并发提交、成组调度与抢占下
得到**一致且可解释**的结论。

## 1. 架构与一致性模型

```
web (REST Controller, 全局异常处理)
        │  只做 DTO 转换
        ▼
service.JobService / ClusterService        ← 统一服务入口，同一把全局调度锁
        │
        ├── scheduler.GangScheduler        成组试算（first-fit，只读）+ 原子提交
        ├── scheduler.PreemptionPlanner    抢占试算（虚拟释放，只读）+ 提交阶段回收
        ├── quota.QuotaManager             按属主的配额准入
        └── ledger.ResourceLedger          进程内集群账本（所有读写同一把锁）
```

关键一致性保证：

1. **单锁串行化调度**：作业提交、查重、调度、抢占回收、节点排空都在 `JobService`
   的同一把锁 + `ResourceLedger` 自身的锁内完成；并发请求被串行化判定，结论确定。
2. **先试算后原子提交，杜绝部分分配**：
   - 成组试算 `planPlacement` / 抢占试算 `planPlacementWithRelease` 都是**只读**的，
     不修改账本；
   - 真正落地走 `commitAtomic`：先按节点聚合全部任务需求并整体复核
     （节点存在、仍为 READY、各类容量足够），全部通过才一次性修改账本；
     任一不满足则账本完全不变。
3. **容量守恒**：账本保证 `0 ≤ used ≤ capacity`；完成作业、抢占、排空驱逐都走
   `releaseAtomic` **整组回收**。`GET /api/nodes/verify` 与
   `ClusterService.verifyInvariants()` 同时做两层校验：
   - 账本自校验：占用非负、不超容量；
   - 交叉校验：所有 RUNNING 作业任务落点聚合出的占用 == 账本占用（检测泄漏/幽灵分配）。

## 2. 作业提交流程与拒绝原因区分

| 情形 | 服务层表现 | HTTP | 原因码 |
|---|---|---|---|
| 字段缺失/需求为负/空任务 | `InvalidRequestException` | 400 | `INVALID_REQUEST` |
| 相同 `idempotencyKey` 重复提交 | `DuplicateSubmissionException`（携带既有作业） | 409 | `DUPLICATE_SUBMISSION` |
| 超属主配额 | 作业 **QUEUED**，不占任何资源 | 200（提交成功） | 作业上记录 `QUOTA_EXCEEDED` |
| 容量不足且无可行抢占 | 作业 **QUEUED** | 200 | 作业上记录 `INSUFFICIENT_CAPACITY` |

幂等语义：幂等键索引与作业创建在同一锁内完成。重复提交**不会**创建第二个作业实例，
**不会**重复占用资源；异常中携带既有作业快照（id、状态、落点、判定日志），可直接区分
“非法请求”与“重复请求”。

## 3. 调度策略

- 每个作业由 1..N 个**任务**组成，每个任务声明对 `CPU / MEMORY(MB) / GPU` 三类资源的需求。
- **成组调度**：一个作业的全部任务必须在同一时刻都找到落点才能 RUNNING；
  否则保持 QUEUED，不产生任何部分分配。多个任务可落在同一节点（只要容量足够）。
- 每轮调度（提交后、作业完成后、节点恢复后自动触发，也可 `POST /api/jobs/schedule`
  手动触发）按 **priority 降序、同 priority 提交时间升序（FCFS）** 处理排队作业。
- 落点策略为跨 READY 节点的 **first-fit**；试算时给出逐任务、逐节点、逐资源的缺口依据，
  以及集群级总缺口（如 `GPU缺1`），写入作业 `decisionLog`。
- **节点健康**：仅 `READY` 节点接收新作业；`UNSCHEDULABLE`（不可调度）与
  `DRAINING`（排空中）一律不参与落点。

## 4. 配额规则

- 配额按 `owner`（属主/用户/租户）配置，逐类资源判定：作业**总需求**任一资源超过配额即
  判定 `QUOTA_EXCEEDED`，作业排队等待，不申请节点资源；判定依据形如
  `owner=team-a MEMORY 需求=20480 超过配额=16384`。
- 未显式配置配额的属主视为无限配额。默认集群中 `team-a=16C/32GB/4GPU`、
  `team-b=8C/16GB/2GPU`。

## 5. 抢占规则

当高优先级作业成组试算失败时进入抢占试算：

1. **只允许抢占严格更低优先级**的 RUNNING 作业；同级、更高优先级作业受保护；
2. 受害者按 **优先级升序，同优先级最晚提交优先** 逐个尝试；
3. 采用“虚拟释放”只读模拟：只有当抢占某个受害者集合后，目标作业的**全部任务**都能
   成组落地时，抢占才成立；否则**一个都不抢**（避免白抢），作业继续排队，
   无任何资源变动；
4. 抢占成立后，在同一调度锁内先对每个受害者 `releaseAtomic` **整组回收资源**，
   受害者回到 QUEUED（累计 `preemptCount`、记录抢占者与可解释依据），再原子提交
   目标作业。
5. 节点排空驱逐（`drain?evictRunning=true`）复用同一条整组回收路径，资源不泄漏。

## 6. REST API

| 方法与路径 | 说明 |
|---|---|
| `POST /api/jobs` | 提交作业（body: idempotencyKey/owner/priority/tasks[]） |
| `GET /api/jobs` / `GET /api/jobs/{id}` | 查询作业（含 state、placement、lastReason、decisionLog） |
| `POST /api/jobs/{id}/complete` | 完成作业，整组回收资源并触发一轮调度 |
| `POST /api/jobs/schedule` | 手动触发一轮调度 |
| `GET /api/nodes` | 节点容量/占用/空闲/状态 |
| `POST /api/nodes/{id}/state?state=READY|UNSCHEDULABLE|DRAINING` | 标记节点状态 |
| `POST /api/nodes/{id}/drain?evictRunning=false` | 排空节点，可选驱逐并回收在跑作业 |
| `POST /api/nodes/{id}/recover` | 恢复 READY 并触发重调度 |
| `GET /api/nodes/verify` | 账本守恒与落点交叉校验，返回 `{conserved, violations[]}` |

提交示例：

```bash
curl -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' -d '{
  "idempotencyKey":"run-001","owner":"team-a","priority":5,
  "tasks":[{"name":"worker-0","cpu":4,"memoryMb":8192,"gpu":1},
           {"name":"worker-1","cpu":4,"memoryMb":8192,"gpu":1}]}'
```

## 7. 验证方法

```bash
mvn test
```

测试共 16 个用例，全部在断言前用日志打印**输入**与**判定依据**（SLF4J，
surefire 报告见 `target/surefire-reports/`）：

| 测试类 | 覆盖场景 |
|---|---|
| `ResourceLedgerTests` | 分配/释放容量守恒；超额与排空节点拒绝；原子提交不留半成品 |
| `JobSubmissionTests` | 非法请求（400 类）与重复提交（409 类）可区分；重复不产生第二实例、资源只占一次 |
| `GangSchedulingTests` | 成组同时满足才启动；不满足时排队且给出 GPU 缺口；配额超额逐类依据；完成后回收再调度 |
| `PreemptionTests` | 高优先级抢占低优先级并完整回收；同级不抢占；抢占后仍无法成组则不白抢 |
| `NodeDrainTests` | 排空节点不接收新作业、驱逐回收、恢复后重新参与调度 |
| `ConcurrentSubmissionTests` | 32 线程并发同幂等键：恰 1 个成功、31 个重复拒绝、单实例、账本守恒 |
| `JobApiIntegrationTests` | HTTP 端到端：200/400/409 原因码可区分，verify 守恒，重复仅一个实例 |

启动示例集群（3 节点：node-a/node-b 各 8C/16GB/2GPU，node-c 4C/8GB/0GPU）：

```bash
mvn spring-boot:run
```

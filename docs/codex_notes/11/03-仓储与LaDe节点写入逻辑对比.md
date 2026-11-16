### 任务背景

对比当前仓储项目与本机 `/data/zc/datasets/LaDe/viewer` 的知识图谱节点写入逻辑，重点核对写入主体、节点类型、历史频率、同步异步、完整状态及 Step 一致性。本次为只读源码分析，没有启动仿真、连接数据库或执行写入压测。

对比时的 Git HEAD：仓储 `3785ef5`；LaDe `5e6f9b2`。旧的 10 月 26 日差距评估不能代表当前实现，本文以这次读取的工作区代码为准。

### 关键结论

- 两边都采用增量更新：实体节点用 MERGE/SET 更新，历史状态和事件按确定标识新增或幂等重写，均不是每步复制整张图。
- LaDe 支持 main、ray、process 三种写入路由；ray 模式下无人机 actor 写自身事件和状态，独立 writer actor 写全局 Step 与跨智能体事实。仓储新路径同样是机器人 actor 写自身子图，但全局记录通过独立 HTTP 服务提交。
- LaDe 在开启 `record_agent_state` 后每步保留每架无人机的 AgentState；仓储新路径每步更新 Robot 完整当前状态，历史 AgentState 默认每 4 步采样，另保存 tick 1。
- LaDe 的完整图谱调度快照是独立 DroneRuntimeState，按 execution_id、drone_id 和操作 version 管理；仓储使用 Robot.payload、last_written_tick，并由 Run.latest_step 标记全体完整帧。
- 仓储 memory 路径可异步积压 120 帧，待 100 台写入回执与全局 Step 实际落库确认后释放；LaDe ray 写入在 actor 返回控制结果前完成，专用 writer 队列用于 Step 和其他委托写入，并非同一套整帧积压机制。
- 仓储把固定 WarehouseTask 与每次运行的 Execution 分开；LaDe 的 Task、Drone、Region、Event 基本按 run_id 隔离，在运行期间由事件增量创建/更新。
- 仓储最近已核对的本地运行只启用 journal，不能把新 Ray 图谱能力视为该运行已经实际启用；LaDe 动态入口默认 memory，headless 默认 neo4j 且记录历史，两者也不能混称默认配置。
- 本文不据代码结构推断两边吞吐或某一方更快；对齐机器人数量、步长、历史采样、写入模式与数据库后才可比较性能。

### 执行过的操作

- 定位 LaDe 目录，读取其 AGENTS.md，并只读访问该项目。
- 读取两边 Git HEAD，检索历史差距评估以确认参考项目身份。
- LaDe：读取 `simulation/knowledge.py`、`simulation/knowledge_writer.py`、`simulation/knowledge_state.py`、`simulation/ray_delivery_sim.py`、`simulation/knowledge_worker.py`、`scripts/run_dynamic_sim.py`、`scripts/run_headless_sim.py`、`run_ray_sim.sh`。
- 仓储：复核 `backend/ray_medium/sim.py`、`backend/robot_actor/kg_state.py`、`backend/robot_actor/graph_async.py`，结合前一轮核对的 `backend/robot_actor/ray_actor.py`、`backend/warehouse_runtime_graph.py`、`backend/global_writer.py` 与 `ops/warehouse_live_runtime.py`。
- 使用 `rg`、`sed`、`cat`、`git rev-parse --short HEAD` 进行静态阅读；没有运行测试，因为本次没有修改实现。

### 修改内容

仅新增本说明文件。未修改两个项目的业务代码、配置、输入、实验结果或数据库。

#### 1. 节点模型与身份

| 方面 | 仓储项目 | LaDe |
|---|---|---|
| 固定业务数据 | Warehouse、Zone、Rack、Location、Dataset、Goods、WarehouseTask，独立导入 | Task、Drone、Region 通常随运行事件 MERGE 创建/更新 |
| 运行身份 | Run 引用 Dataset；固定任务可被不同 Run 的 Execution 引用 | 主要节点带 run_id；同一业务任务在不同运行中是不同图谱身份 |
| 当前实体 | Robot，每步更新位置、电量、阶段等；新路径另有完整 payload | Drone，在 actor 写入模式每步更新当前属性 |
| 历史实体 | AgentState、ExecutionState、RuntimeEvent | AgentState、Event |
| 全局事实 | Step、RunStat、DispatchState、Assist、Handoff 等 | Step，以及 ASSISTS、HANDOVER_TO、TAKES_OVER 等关系和事件 |
| 完整调度状态 | Robot.payload JSON + last_written_tick | 单独 DroneRuntimeState.payload_json + execution_id/version/schema_version |

LaDe 的 `_write_event()` 会同时 MERGE Run、Region、Drone、Task、Event 并维护 ASSIGNED/CARRYING 等关系；Task 的业务状态随事件覆盖。仓储将静态 WarehouseTask 和本次 Execution 分离，Execution 通过 EXECUTES 关联静态任务，运行事实单独记录。这是业务图模型差异，不能仅按相同“任务数”比较图节点总量。

两边都依赖标识和 MERGE 做幂等。LaDe 常使用 `(run_id, id)` 等复合身份；仓储大量运行态 id 编入 runId、机器人号、tick 或事件编号。幂等不代表跨所有节点的一次写入是单个事务。

#### 2. 谁实际写数据库

LaDe 三种模式由 `--kg-write-mode` / `KG_WRITE_MODE` 选择：

- `main`：主进程 Neo4jKnowledgeStore 直接同步写事件和 Step；启用历史时集中 UNWIND 写该步所有 AgentState。
- `process`：主进程保留内存显示历史，数据库命令交给独立 `knowledge_worker` 子进程；使用有界 FIFO，默认 128 条命令，支持回执、flush、错误传播。
- `ray`：每架无人机 actor 配置自己的 Neo4j store，写任务分配、取货、完成、无人机状态；另一个 RayKnowledgeWriter actor 写 Step、跨智能体互助等全局事实。主进程避免重复写已经由无人机 actor 写过的事件。

仓储较新路径由 `--robot-processes ray --robot-graph --global-writer-url URL` 启用：

- 每台 RobotActor 通过自己的 writer 写自身历史、事件和完整当前状态。
- 全局 Step、RunStat、交接、负责人变更等交给 HTTP 全局记录服务，由该服务写库。
- 主进程只在启动时准备共享 Run/schema，再将机器人写入分散，避免机器人每次直接更新共享 Run 的全局统计。
- 旧 `--ray` MotionActor 和旧 `--global-writer` 子进程路径仍保留，不具备全部较新路径语义；普通 journal 模式不会自动启用这些图谱写入。

LaDe 的 `main` 模式是默认写入路由，但是否连接数据库仍由 KG_BACKEND 决定。动态脚本默认 KG_BACKEND=memory；headless 默认 neo4j。仓储较新入口默认 knowledge-backend=journal，机器人图谱与全局服务开关独立控制 C 套运行态写入。不要把 memory/ray 取状态与 memory/neo4j 存储后端混为一谈。

#### 3. 每步产生多少历史节点

| 数据 | 仓储较新路径 | LaDe Neo4j 路径 |
|---|---|---|
| 当前 Robot/Drone 属性 | 每步更新 | ray 写入模式每步更新；main/process 的无人机属性随事件或开启的状态记录更新 |
| 历史 AgentState | 默认每 4 tick，另有 tick 1；可设 interval=1 | record_agent_state 开启后，每个 step 每架一份，无默认每四步采样 |
| 事件节点 | 有真实业务事件时写 | 有真实业务事件时写 |
| Step | 新全局服务路径每 tick | 每个环境 step 都调用 record_step；具体提交路由由模式决定 |
| 派单快照 | DispatchState 当前每 5 tick 构建 | 不存在相同职责的 DispatchState；完整读图状态使用 DroneRuntimeState |

按相同 100 个实体、10000 个仿真步粗略比较，仅历史 AgentState：LaDe 开启历史约 100 万份，仓储 interval=4 约 25 万份，另计起始/特殊采样帧。这里不是总节点数，更不是性能实测。仓储 interval=1 时同样可以逐步累积约百万历史状态。

步数必须与时间区分：仓储当前每步 1 仿真秒；LaDe 由 `--step-seconds` 决定，动态脚本默认 20 秒，headless 默认 6 秒。因此同一步数并不代表同一段仿真时间或同一写入速率。

LaDe 的通用 store/config 默认 `record_agent_state=False`，动态入口由 `KG_RECORD_AGENT_STATE=1` 开启；headless 默认开启 `--record-agent-state`。不能说 LaDe 无条件每步新增 AgentState。

#### 4. 当前状态与历史快照的区别

LaDe：

- Drone 是业务当前节点；AgentState 是查询历史的采样摘要。
- 只有选择 `--state-source neo4j` 时，才启用完整 DroneRuntimeState 作为运行数据通路。它包含无人机完整快照、队列/载货任务 payload 和任务更新。
- DroneRuntimeState 按 `(run_id, execution_id, drone_id)` 唯一，后续操作更新同一节点。恢复环境会生成新的 execution_id，避免与此前执行混用。
- 除每步推进外，派单、交接等 `_call_actor()` 操作也可以生成新的 version，故完整调度快照更新可能超过“每物理步一次”。

仓储：

- 新 RobotActor 每次写历史/事件后，调用 record_robot_current_state 更新现有 Robot.payload 和 last_written_tick。
- 图谱派单读取要求 Run.latest_step 至少到达所需 tick，并要求恰好 100 台 Robot 的 last_written_tick 等于该 tick、payload 字段与预期集合完全匹配。
- memory 模式仍可写这些完整状态，但派单用 actor 返回的内存状态；neo4j 模式才同步等待并读图。
- Robot.payload 是最新状态，不是每 tick 的完整可回溯副本；历史 AgentState 只包含相应历史摘要。不能仅凭 Step 都存在就认为能恢复任意历史时刻的完整运行现场。

#### 5. Step 与落库确认

LaDe ray 写入：

```text
actor 推进
  → 同步写业务事件、Drone 与可选 AgentState
  → actor 返回结果
  → 主进程收齐 actor 结果
  → 全局 writer 写 Step，并关联本步 AgentState
```

Step 的顺序主要由 actor 调用返回与 writer.flush 屏障保障。main 集中写历史时，Step 与该批 AgentState 在同一条 Cypher 中写入；ray 模式已有 AgentState 后，再由全局 writer 写 Step/IN_STEP 关系。

读取最新 Step 是查询 `max(step_index)`；record_step_data 没有仓储式“期望 100 台/实际机器人编号/complete”门槛或 Run.latest_step 属性。其正确性主要依赖调用链顺序，不能说完全没有同步保障。

仓储新异步路径：

```text
主循环推进并保存不可变帧
  → 有界 FIFO 后台逐帧处理
  → 100 台 actor 并行写自身图谱并返回实际成功回执
  → 提交全局 HTTP 记录
  → 服务写全局事实，再发布 complete Step 与 Run.latest_step
  → 后台确认 latest_step 已推进，释放该帧额度
```

当前默认 backlog=120，达到上限会在下一物理步之前等待全部积压追平。HTTP 202 只算接收，不能释放额度。写入失败会传播并暂停该路径。neo4j 取状态模式则同步执行，避免用落后图谱派单。

LaDe 的委托 writer 也有有界队列与确认，但其 128 是写命令/待完成请求数量，不等于仓储的 120 个完整仿真帧。LaDe 读委托知识库前通常 flush；headless 保存 checkpoint 前也会先 flush，让持久化检查点至多比图谱 Step 超前一步。

#### 6. 源码定位

LaDe：

- [写入路由与配置](/data/zc/datasets/LaDe/viewer/simulation/knowledge_writer.py:27)
- [委托写入与 Step](/data/zc/datasets/LaDe/viewer/simulation/knowledge_writer.py:352)
- [Step 与历史 AgentState](/data/zc/datasets/LaDe/viewer/simulation/knowledge.py:577)
- [actor 当前 Drone 与历史写入](/data/zc/datasets/LaDe/viewer/simulation/knowledge.py:653)
- [完整 DroneRuntimeState](/data/zc/datasets/LaDe/viewer/simulation/knowledge.py:293)
- [完整状态版本校验](/data/zc/datasets/LaDe/viewer/simulation/knowledge_state.py:38)
- [actor 每步先写再返回](/data/zc/datasets/LaDe/viewer/simulation/ray_delivery_sim.py:227)
- [主循环最后提交 Step](/data/zc/datasets/LaDe/viewer/simulation/ray_delivery_sim.py:546)

仓储：

- [actor 当前完整状态写入](/data/zc/datasets/human_robot/warehouse-dashboard/backend/robot_actor/ray_actor.py:46)
- [逐步不可变帧](/data/zc/datasets/human_robot/warehouse-dashboard/backend/robot_actor/ray_actor.py:293)
- [100 台回执与全局 Step 确认](/data/zc/datasets/human_robot/warehouse-dashboard/backend/robot_actor/ray_actor.py:341)
- [AgentState 历史采样](/data/zc/datasets/human_robot/warehouse-dashboard/backend/warehouse_runtime_graph.py:153)
- [当前 Robot.payload](/data/zc/datasets/human_robot/warehouse-dashboard/backend/warehouse_runtime_graph.py:202)
- [完整 Step 发布](/data/zc/datasets/human_robot/warehouse-dashboard/backend/global_writer.py:342)
- [同步图谱状态校验](/data/zc/datasets/human_robot/warehouse-dashboard/backend/robot_actor/kg_state.py:19)

### 后续建议

- 如要对齐 LaDe 的逐步历史节点增长，应显式开启仓储图谱路径并设置 graph-state-interval=1，同时对齐双方仿真步长；本次没有改动。
- 如要比较写入性能，分别测当前实体更新、历史 AgentState、事件、全局 Step 的数量及耗时，固定同一硬件与数据库参数；不要只比较总节点数。
- 如要比较读图派单可靠性，分别检查 LaDe 的 execution_id/version 校验和仓储的完整 Step/last_written_tick 校验，不使用历史 AgentState 代替完整当前调度状态。

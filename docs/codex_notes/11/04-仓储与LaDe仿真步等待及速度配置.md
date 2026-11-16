# 仓储与 LaDe 仿真步等待及速度配置

### 任务背景

用户希望理解当前仓储程序与本机 LaDe 的运行逻辑，重点是进入下一仿真步需要等待哪些条件，并判断 `ray` 状态源与 `process` 写入是否最快。本次只读分析源码及已保存运行配置，没有更改或重启实验。

### 关键结论

- 两边都是主协调器派单、Ray actors 推进实体、主协调器收齐结果的逐步仿真；不需要等一个业务任务全部完成才推进下一步。
- 仓储物理步长固定为 1 仿真秒。当前最新保存配置 `1104-6192tasks` 为 `kg_state_source=neo4j`、`kg_write_mode=process`、`graph_state_interval=1`、`commit_seconds=5`。本次没有实时查询进程，不能保证该记录始终就是页面当前运行。
- 当前三种写路由都等待回执。`main/process` 由所选 writer 顺序调用各机器人写入，`ray` 由机器人 actors 并行提交各自子图，协调器收齐写入回执后才提交全局事实和完整 Step。
- `neo4j` 状态源额外要求 `Run.latest_step >= 当前 tick`，并成功读回本 tick 的 100 台机器人完整 payload，再开始本步派单；缺失、错误或超时不回退内存。
- `ray` 状态源在内部对应 `memory`，省去上述状态回读，但新 `kg_write_mode` 路径仍同步等待机器人写图和全局 Step；状态来源和写入模式是两个独立维度。
- `process` 不等于更快或异步。它提供独立进程写入，仍通过 pipe 等结果；`main` 省去这层通信，`ray` 支持并行机器人写入，谁最快需要同输入实测，不能仅凭模式名判断。
- 旧 HTTP 全局服务路径的有界异步写图仍保留，但仅满足无新 graph_writer、memory 状态源、启用 robot_graph、存在 global_writer_url 等条件才生效；它不由当前启动器的 `ray + process` 组合自动启用。
- LaDe 与仓储的完整状态检查不同：LaDe 用 `execution_id` 和每个实体的 `version` 校验 `DroneRuntimeState`；仓储用完整 Step 水位及 Robot 的精确 `last_written_tick` 校验整帧。

### 执行过的操作

- 阅读仓储 `backend/robot_actor/ray_actor.py`、`local.py`、`kg_state.py`、`graph_async.py`。
- 阅读 `backend/scheduling_v3/sim.py`、`backend/scheduling_v5/sim.py`、`backend/wide/sim.py`、`backend/knowledge_runtime/sim.py` 的步进和调度逻辑。
- 阅读 `backend/warehouse_knowledge_writer.py`、`backend/global_writer.py`、`ops/warehouse_live_runtime.py`、`ops/start_neo4j_live.py`。
- 使用 `./run_in_lade.sh python -B` 只读解析最新三个 `data/neo4j-runs/*/live-manifest.json`，仅输出相关配置字段。
- 阅读 `/data/zc/datasets/LaDe/viewer/AGENTS.md`，只读参考其 `simulation/ray_delivery_sim.py`、`knowledge_writer.py`、`knowledge.py`、`knowledge_state.py` 和动态/headless 入口。
- 使用 `rg -n`、`sed -n` 定位调用链，没有运行仿真或性能测试，没有修改 LaDe。

### 修改内容

- 仅新增本文档；没有修改当前运行、数据库、启动参数或业务代码。

### 后续建议

#### 当前配置每步如何推进

假设已经处于 tick=t，准备推进到 t+1：

1. 实时 worker 处理控制命令和到期扰动；如果存在必须保存的知识边界，先同步保存。
2. `neo4j` 模式在派单入口检查 t 帧：必要时先发布机器人状态和全局 Step，随后等待 `Run.latest_step >= t`；精确查询 `Robot(run_id, last_written_tick=t)`，校验 100 台机器人身份和全部状态字段。读取器默认总期限为 2 秒，失败则抛错停止推进。
3. 释放到期任务，执行需要的调度和路径/资源预约。调度轮次有自己的间隔，并非每个仿真秒都完整重算任务匹配。
4. 向全部机器人 actor 发出本秒推进调用，使用一次 `ray.get(refs)` 等所有返回。空闲、充电、排队的机器人也返回本秒状态，不要求完成任务。主进程按稳定顺序合并事件，交接改变后续机器人输入时可能补做对应计算。
5. 物理引擎把 elapsed/tick 增加 1，执行运动、能量、容量等检查。
6. 生成 t+1 的机器人图谱快照。process 模式等待独立 writer 完成各机器人写入；ray 模式等待机器人 actor 写图回执。全局 Step 的完整条件是收齐本 tick 所有 100 台机器人成功回执。全局 writer 写跨机器人事实后，最后写 `Step.complete=true` 并更新 `Run.latest_step`；新写路由同步等其返回。
7. 若到达保存间隔（此记录为 5 个仿真秒）、特殊边界或结束条件，再同步提交 Neo4j 检查点/事件批次及本地诊断文件，发布页面快照。
8. worker 按 `wall_interval / speed` 安排下一次调用。计算或写库超时会降低实际速度，设置高倍速不能绕过计算和持久化条件。慢步后允许有限度追赶。

注意：物理 tick 在写图前已经增加，写图失败不表示该步物理计算从未执行，不能直接重放这一秒来补写。进程异常由 live worker 转为失败状态；不能假设点击继续就能安全重试。

#### 和 LaDe 的对应关系

| 项目 | 当前仓储新启动路径 | 本机 LaDe |
|---|---|---|
| 物理执行 | 每机器人一个 Ray actor，收齐结果 | 每无人机一个 Ray actor，收齐结果 |
| 仿真步长 | 固定 1 秒 | `--step-seconds` 可配置；本次源码中 dynamic 默认 20 秒，headless 默认 6 秒 |
| 内存状态源 | 外部 `ray`，内部 `memory` | `ray` |
| 图谱状态源校验 | 完整 Step + 本 tick 的 100 台 Robot payload | execution_id + 各无人机预期 version + 完整 DroneRuntimeState |
| process 写入 | 本次请求等待进程回执 | 可委托入队，flush 等累计请求完成 |
| ray 写入 | 机器人 actors 写自身子图，协调器等回执，全局 writer 同步提交 Step | 无人机 actor 可在返回前完成自身写入，全局 writer 可委托提交 |

LaDe 的 `step_once()` 顺序是派单、所有 actor 前进、收齐返回、必要时 flush 并从图谱读完整版本状态、合并事件和帧、可选检查点钩子、记录 Step。委托写入的 `record_step()` 本身是 submit，不应笼统声称函数返回时 Step 必已落库；后续 flush、图谱查询、检查点或队列满会等待。ray 写模式的下步派单入口主动 flush；neo4j 状态源的运行状态查询也会先 flush。主进程直接写入模式则同步执行数据库调用。

#### 为什么 ray + process 不能保证最快

- `KG_STATE_SOURCE=ray` 通常减少每步数据库回读成本，但也意味着调度状态直接来自 actor 返回，而不是通过 Neo4j 校验读取，这是实验语义的变化。
- `KG_WRITE_MODE=process` 在本仓储实现中一次调用发给独立进程后立即等结果，不能让本步计算与该次写入自然重叠；还存在 payload 复制、序列化和进程通信成本。
- `main` 不需要跨进程往返；`ray` 可以并行写各机器人子图，但会受到连接数、锁竞争和 Neo4j 写吞吐影响。不能未测就排名。
- `record_agent_state` 和 `graph_state_interval` 控制历史 AgentState 的写入量；即使降低历史采样频率，完整当前状态及 Step 的同步要求仍保留。改变采样会影响历史追溯粒度，需要按实验目标选择。
- 当前代码会把请求级 `robot_graph_async=true` 写入 manifest；有效值由 sim 构造条件重新计算。存在 graph_writer 或 neo4j 状态源时实际异步为 false，不能仅看 manifest 的该布尔字段判断正在异步推进。

如果要确定性能最优组合，应在独立新 run 中，固定任务集、物理配置、CPU、历史采样、状态来源及数据库条件，对比三种 writer 的实际仿真秒/墙钟秒和阶段耗时。可看 `loop_timing`、`persist_timing.jsonl` 及已有 `kg_timing` 指标，区分运动/调度、写图、等 Step、回读和检查点耗时；不要在当前实验中直接改配置做混合成绩。本次没有自动发起长时间基准，也没有待用户批准的变更。

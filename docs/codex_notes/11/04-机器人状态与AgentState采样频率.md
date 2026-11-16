# 机器人状态与 AgentState 采样频率

### 任务背景

用户希望确认当前机器人状态采集频率，及其与 LaDe 每 step 保存一次 AgentState 的关系。本次只读核对源码与最新保存的运行配置。

### 关键结论

- 最新保存的运行配置为 `data/neo4j-runs/6912_fast_1104/live-manifest.json`，其中 `record_agent_state=true`、`graph_state_interval=1`，因此历史 AgentState 按每机器人每仿真步保存，与用户所说的 LaDe 逐步记录频率一致。
- 仓储当前每步为 1 仿真秒，因此这套配置相当于每机器人每仿真秒保存一份历史状态。它不是每现实秒采集一次；实际采集墙钟频率随仿真速度变化。
- `Robot` 当前状态与历史 `AgentState` 分开：当前路径每步更新已有 Robot 节点的 `payload` 和 `last_written_tick`；历史 AgentState 则按 run、机器人、tick 身份累积。
- 历史写入条件为 `record_agent_state and (tick % state_interval_ticks == 0 or tick == 1)`。间隔 4 时记录已提交的 tick 0、1、4、8 等采样边界；间隔 1 时每步都记录；关闭 record_agent_state 后不保存历史 AgentState，但当前状态仍更新。
- `commit_seconds=5` 是检查点和事件批次的保存间隔，不是 Robot 或 AgentState 的采样间隔。
- 本次还看到该最新记录使用 `kg_state_source=neo4j`、`kg_write_mode=main`。名称中的 fast 不意味着实际已启用 ray 状态源；没有实时查询服务，不能仅凭文件修改顺序认定它始终是页面当前运行。

### 执行过的操作

- 阅读 `backend/warehouse_runtime_graph.py:record_robot_snapshot()`、`record_robot_current_state()`，核对采样条件和当前状态更新方式。
- 阅读 `backend/robot_actor/ray_actor.py:_emit_robot_graph()` 和 `backend/warehouse_knowledge_writer.py:WriteExecutor.apply()`，确认每步快照写入路径。
- 通过 `./run_in_lade.sh python -B` 只读解析最近两个 `data/neo4j-runs/*/live-manifest.json` 的相关配置字段。
- 对照 LaDe `simulation/ray_delivery_sim.py` 中无人机 actor `step()` 调用 `record_drone_state()` 的路径，以及 `simulation/knowledge.py` 中历史开关条件。
- 使用 `rg -n` 和 `sed -n` 检查初始化默认值，没有运行实验、数据库查询或测试。

### 修改内容

- 仅新增本文档，没有修改状态采样配置或当前运行。

### 后续建议

| 数据 | 最新保存配置下的频率 | 保存方式 |
|---|---|---|
| Robot 当前状态 | 每仿真步 | 更新已有节点 |
| AgentState 历史 | 每仿真步，interval=1 | 按机器人与 tick 保留历史节点 |
| 检查点/事件批次 | 通常每 5 仿真秒，特殊边界额外保存 | 单独的持久化提交 |

100 台机器人运行 1000 个新的仿真步，按此配置会产生约 10 万个对应步的历史 AgentState，初始边界另计。不会每步新增 100 个 Robot 身份节点。

若只比较每步频率，当前与 LaDe 相同；若比较每小时仿真时间的历史数量，还要对齐 LaDe 的 step_seconds，不能把双方一 step 当成相同秒数。LaDe 开启历史记录是前提。

本次无需用户确认。

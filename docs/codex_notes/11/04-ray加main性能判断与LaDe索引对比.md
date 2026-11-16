# ray 状态源加 main 写入的性能判断与 LaDe 索引对比

### 任务背景

用户希望判断 `ray` 状态源配 `main` 写入是否最快，确认仓储写 Neo4j 时是否已有索引，并比较本机 LaDe 的索引实现。本次仅检查两边源码，没有修改配置、连接数据库或运行性能实验。

### 关键结论

- 不能确认 `ray + main` 最快。ray 状态源省去图谱状态回读；main 省去写入路由的跨进程通信，但当前实现会顺序写各机器人，ray 写模式可让机器人 actors 并行写自身子图。数据库吞吐、网络往返和锁竞争共同决定结果。
- 仓储已有自动索引机制：静态导入、运行图谱初始化、检查点存储分别创建索引/约束；使用 `IF NOT EXISTS`。写入后的索引维护由 Neo4j 完成，不会每步重建整套索引。
- 源码存在建索引机制不能证明指定数据库中索引已 ONLINE；本次未检查 17474/47474 对应库。应在实际业务库执行 `SHOW INDEXES` 验证。
- 两边都使用 Neo4j 原生属性索引及唯一约束支撑索引，机制相近；字段和覆盖范围依业务模型而异。仓储覆盖静态资源、机器人当前状态、执行、交接、事件、统计与检查点；LaDe 本次检查的普通属性索引主要为 AgentState 的运行/无人机/区域/步号查询。
- LaDe 的无人机、任务等常用 `(run_id, id)` 复合唯一约束，完整状态使用 `(run_id, execution_id, drone_id)`；仓储运行态多用编码了 runId 的全局唯一 id，并建立 run_id 与 tick、task_id 等复合查询索引。
- LaDe 历史索引初始化会检查等价索引、同名不同定义、FAILED 状态，并轮询到 ONLINE。仓储基础运行 schema 主要创建定义；可选上下文 schema 另调用 `db.awaitIndexes(60)`，就绪检查策略存在差别。
- 更多索引不代表写得更快。索引可减少 MERGE 和匹配节点时的查找开销，同时插入或修改被索引字段也需要维护索引，吞吐结论应以相同配置实测为准。

### 执行过的操作

- 阅读仓储 `backend/warehouse_knowledge_writer.py:WriteExecutor.apply()`，确认 main/process 逐机器人写入及路由回执行为。
- 阅读 `backend/robot_actor/ray_actor.py:_emit_robot_graph()`，确认 ray 写入走机器人 actors 并行提交。
- 阅读 `backend/warehouse_graph.py`、`backend/warehouse_runtime_graph.py`、`backend/query_c/catalog.py`、`backend/neo4j_live_store.py` 的索引与约束定义。
- 只读参考 `/data/zc/datasets/LaDe/viewer/simulation/knowledge.py` 的 `AGENT_STATE_INDEXES`、`ensure_writer_constraints()`、`ensure_runtime_state_schema()`、`ensure_agent_state_indexes()`。
- 阅读 LaDe `simulation/knowledge_writer.py:KnowledgeWriterConfig`，核对自动创建索引的配置条件。
- 使用 `rg -n`、`sed -n` 定位并核对源码；未运行测试，未进行数据库写入或性能排名测试。

### 修改内容

- 仅新增本文，未修改仓储或 LaDe 业务代码。

### 后续建议

#### 主要索引的对应关系

| 用途 | 仓储 | LaDe |
|---|---|---|
| 运行身份 | Run.id 唯一约束 | Run.id 唯一约束 |
| 实体身份 | Robot.id、Execution.id、RuntimeEvent.id 等唯一约束 | Drone、Task、Region、Event、Step、AgentState 的 (run_id, id) 唯一约束 |
| 单智能体历史 | AgentState(robot_node_id, tick)；上下文模块另建 (run_id, robot_id, tick) | AgentState(run_id, drone_id, step_index) |
| 整场历史 | AgentState(run_id, tick) | AgentState(run_id, step_index) |
| 区域历史 | 本次检查的基础运行 schema 无直接对应项 | AgentState(run_id, region_id, step_index) |
| 完整当前状态 | Robot(run_id, last_written_tick) 查询索引，加 Robot.id 唯一约束 | DroneRuntimeState(run_id, execution_id, drone_id) 唯一约束 |
| 任务与交接 | Execution(run_id, task_id)、OwnershipChange(run_id, task_id, tick) 等 | 本次检查路径主要通过 Task 身份约束及事件关系查询 |
| 静态仓储资源 | Warehouse/Rack/Location/Goods 等 ID 约束；WarehouseTask(dataset_id, task_id) 索引 | 不同业务模型，无直接一一对应 |
| 无 journal 检查点 | WarehouseLiveCheckpoint(run_id)、WarehouseLiveCommit(run_id, seq) 唯一约束 | 本次所检查的 LaDe schema 无同名对应项 |

不要只按数量评价覆盖是否充分，也不要把 schema 定义视为某次运行已经安装全部可选索引。

LaDe `KnowledgeWriterConfig` 默认 `auto_create_indexes=True`，环境变量 `KG_AUTO_CREATE_INDEXES` 默认 1；历史 AgentState 普通索引的自动初始化实际还要求 `bootstrap` 和 `record_agent_state`。底层 `Neo4jKnowledgeStore` 构造参数本身默认 false，应以入口传参为准。写入唯一约束和 neo4j 完整状态约束由各自初始化路径处理，不能把历史索引开关理解为所有约束的总开关。

#### 用户可执行的只读核验

在 Neo4j Browser 选择实际业务库后执行：

```cypher
SHOW INDEXES
YIELD name, type, labelsOrTypes, properties, state, populationPercent
RETURN name, type, labelsOrTypes, properties, state, populationPercent
ORDER BY name;
```

`ONLINE` 表示已可用。若需要检查唯一约束，执行 `SHOW CONSTRAINTS;`。查询是否用到索引，可对具体查询执行 `EXPLAIN`。

#### 性能比较范围

保留相同输入、100 台机器人、相同 CPU 配额和历史采样要求，以新 run 对比 `ray+main`、`ray+process`、`ray+ray` 的实际仿真秒/墙钟秒。比较时排除或单独报告初始化/索引构建时间。当前没有实测证据支持任何一个组合必然最快，未改当前实验，也没有需要用户确认的变更。

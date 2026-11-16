# Neo4j 自动索引机制核对

### 任务背景

用户希望确认现有系统是否在向 Neo4j 存数据时自动构建索引。本次检查现有代码定义和启动调用路径，没有连接数据库执行建索引操作。

### 关键结论

- 已接入：应用在导入或写入初始化阶段声明索引与约束，Neo4j 负责构建及后续随数据增删改自动维护；不是每写入一个节点就重新创建整套索引。
- 静态图谱定义 7 项 ID 唯一约束，以及 `WarehouseTask(dataset_id, task_id)` 复合索引；导入脚本实际写入前调用 `create_schema()`。
- 运行图谱的 `RUNTIME_SCHEMA` 定义 12 项 ID 唯一约束和 12 项普通复合索引。`Run.id` 的唯一约束也提供索引，可用于运行编号定位。
- Ray 机器人写图初始化路径调用 `bootstrap_robot_graph()`，再执行 `ensure_schema()` 或写路由的 `bootstrap`。机器人启动后复用已初始化的 schema；独立写图路径也有首次写入检查。全局 writer 另有首次使用的 schema 初始化。
- 无 journal 检查点存储初始化时创建 `WarehouseLiveCheckpoint(run_id)` 和 `WarehouseLiveCommit(run_id, seq)` 唯一约束。唯一约束有对应的支撑索引。
- 代码均使用 `IF NOT EXISTS`，创建目标是当前连接所选的具体数据库。不会因此给其他实例或其他库创建索引，也不会自动为任意新属性推荐索引。
- 本次确认的是代码行为，尚未核实 17474 或 47474 对应数据库中索引的实际存在及 ONLINE 状态。

### 执行过的操作

- 使用 `rg -n` 搜索 `CREATE INDEX`、`CREATE CONSTRAINT`、`ensure_schema`、`create_schema` 及调用路径。
- 阅读 `backend/warehouse_graph.py`、`scripts/import_warehouse_neo4j.py`、`backend/warehouse_runtime_graph.py`。
- 阅读 `backend/ray_medium/sim.py`、`backend/robot_actor/ray_actor.py`、`backend/warehouse_knowledge_writer.py`、`backend/global_writer.py`、`backend/neo4j_live_store.py`。
- 阅读 `ops/start_neo4j_live.py`，确认启动器默认执行静态导入并启用机器人图谱；`--skip-import` 会跳过静态导入。
- 未修改业务代码，未启动仿真，未执行数据库写入或测试；本次为现有实现说明。

### 修改内容

- 仅新增本说明文件。

### 后续建议

在 Neo4j Browser 中选择实际业务数据库后执行以下只读命令：

```cypher
SHOW INDEXES
YIELD name, type, entityType, labelsOrTypes, properties, state, populationPercent
RETURN name, type, entityType, labelsOrTypes, properties, state, populationPercent
ORDER BY name;
```

`ONLINE` 表示索引可用，`POPULATING` 表示仍在构建，`FAILED` 表示构建失败；可结合 `populationPercent` 查看进度。

查看约束：

```cypher
SHOW CONSTRAINTS;
```

运行图谱主要普通索引：

| 节点 | 字段 |
|---|---|
| RunStat | run_id, tick |
| Assist | run_id, donor_id |
| DispatchState | run_id, commit_seq |
| Step | run_id, tick |
| Handoff | run_id, tick |
| OwnershipChange | run_id, task_id, tick |
| AgentState | run_id, tick |
| AgentState | robot_node_id, tick |
| Robot | run_id, last_written_tick |
| Execution | run_id, task_id |
| ExecutionState | run_id, task_id, tick |
| RuntimeEvent | run_id, task_id |

额外的机器人上下文查询 schema 在启用对应功能时创建，见 `backend/query_c/catalog.py` 及 `ensure_context_schema()`；不能把所有可选模块的定义都视为当前数据库已安装的索引。

存在索引不等于每条查询都会使用索引。如要评估具体查询，可先用 `EXPLAIN` 查看执行计划。本次无需用户确认。

# Ray 上下文查询与现有索引对照

> 本文记录 v2 的历史核对。当前代码已升级 v3，Dataset 门槛和快照查询条件已变化，最新对照与实时访问限制见 [11 月 16 日重新核对](16-w系列与索引重新核对.md)。

核对对象：`scripts/warehouse_llm_context_queries_ray.py`。索引证据为用户在 `warehouse-million-1109/neo4j` 执行并贴出的 `SHOW INDEXES`，共 32 项，全部 `ONLINE`：2 个 LOOKUP、30 个 RANGE。本次没有连接目标库，没有执行 DDL 或修改查询。

## 关键结论

- 默认查询包 `core` 只包含 `w0`、`w5`；它们主要的 Run、Step、Robot、AgentState、RuntimeEvent 属性访问已有匹配索引，不需要因为索引名称不同而重复建索引。
- 全部 `w0～w13` 的过滤组合尚未完全覆盖。较明显的缺口是调度快照的 `(run_id,dataset_id,tick)`、货物事件的 `(run_id,cargo_id,tick)`。任务事件、任务/货物接力查询可以利用部分现有索引，但仍有过滤项需要读出候选节点后处理。
- `Dataset`、`WarehouseTask` 没有出现在提供的索引清单中。是否缺静态节点和 `USES_DATASET` 关系需要另外核实；索引本身不会补充数据。历史复制报告未包含这些静态节点，若当前库仍如此，`w0` 会使后续查询以 `anchor_unavailable` 跳过。

这里的“覆盖”只指存在可匹配的属性访问路径，不表示查询已实测选用索引，也不表示可以消除所有过滤、排序和聚合。Cypher 查询未使用 `USING INDEX` 强制提示；缺少某种索引不等于语法报错。

## 实际调用链

1. `scripts/warehouse_llm_context_queries_ray.py:18` 导入 `read_context`，Actor 的 `execute` 调用它。
2. `backend/robot_operation_context.py:561` 建立只读 session；`execute_context` 逐条自动提交查询。
3. 全部 14 条 Cypher 来自 `config/warehouse-context-queries.json`，目录版本为 `warehouse-operation-context-v2`。
4. `backend/robot_operation_context.py` 中 `PACKS['core'] = ('w0','w5')`；`--queries all` 才选择全部查询。按任务查询且未给任务 ID 时，入口还可能自动加入 `w1`。

## 已有的主要访问路径

| 标签和属性 | 用户清单中的索引 | 对应查询 |
|---|---|---|
| `Run(id)` | `warehouse_runtime_run_id` | 全部查询的运行锚点 |
| `Step(run_id,tick)` | `warehouse_runtime_step_run_tick` | w0、w8 |
| `Robot(id)` | `warehouse_runtime_robot_id` | w0、w5、w8 的指定机器人 |
| `AgentState(robot_node_id,tick)` | `warehouse_runtime_state_robot_tick` | w0、w4、w5、w7、w8、w12 |
| `RuntimeEvent(run_id,robot_node_id,tick)` | `query_c_event_node_tick` | w5、w9、w10 |
| `RuntimeEvent(run_id,task_id)` | `warehouse_runtime_event_run_task` | w2、w3、w6；tick/type 另行过滤 |
| `ExecutionState(run_id,task_id,tick)` | `warehouse_runtime_execution_state_run_task_tick` | w3、w11；status 另行过滤 |
| `OwnershipChange(run_id,task_id,tick)` | `warehouse_runtime_ownership_run_task_tick` | w3 |
| `Handoff(run_id,tick)` | `warehouse_runtime_handoff_run_tick` | w3、w7、w11；task_id/goods_id/参与者另行过滤 |

`AgentState` 实际查询使用的是 `robot_node_id`，不是 `robot_id`。现有 `warehouse_runtime_state_robot_tick(robot_node_id,tick)` 可以定位机器人及时间范围，`run_id` 作为额外条件检查。因此不能仅因缺少 `(run_id,robot_node_id,tick)` 就断言百万状态节点必须全扫；`query_c_state_robot_tick(run_id,robot_id,tick)` 则不是这些语句相同字段的替代品。

## 逐条对照

以下每行均省略共同的 `Run(id)` 锚点。静态数据相关查询还受下一节的数据前提约束。

| 查询 | 已覆盖部分 | 缺口或剩余工作 |
|---|---|---|
| w0 `basis` | Step 的运行和 tick；Robot.id；AgentState 的机器人和 tick | AgentState 的 run_id 额外校验；tick 相同情况下的 id 排序；Dataset 从 Run 关系展开，必须实际存在 |
| w1 `task_pool` | Run 锚点和 Dataset 关系路径 | 缺 `DispatchState(run_id,dataset_id,tick)`；现有 `(run_id,commit_seq)` 不等价 |
| w2 `task_geometry` | RuntimeEvent 的运行和任务；其余静态实体通过关系访问 | 任务匹配只有 task_id；事件 tick/type 需额外过滤和排序；无静态任务属性索引 |
| w3 `task_evidence` | ExecutionState、OwnershipChange 的运行/任务/tick；RuntimeEvent 的运行/任务；Handoff 的运行/tick | Handoff.task_id、事件 tick/type 另行过滤；无静态任务属性索引 |
| w4 `peer_states` | AgentState 的机器人/tick | Robot 按 run_id 枚举；`query_c_robot_run_id(run_id,robot_id)` 可作为结合 `robot_id <> $robot_id` 的候选路径，需查看计划；无需给状态重复建索引 |
| w5 `self_feedback` | Robot.id；AgentState 的机器人/时间窗；RuntimeEvent 的运行/机器人/时间窗 | id 次级排序和属性读取仍需处理，无明显的主要索引缺口 |
| w6 `task_timing` | RuntimeEvent 的运行/任务 | 无 `WarehouseTask(dataset_id,task_id)`；事件 tick 另行过滤，聚合需要处理匹配事件 |
| w7 `cargo_history` | Handoff 的运行/tick；持货机器人状态通过 AgentState 的机器人/tick | 缺 RuntimeEvent 的 cargo_id 组合；缺 Handoff 的 goods_id 组合；Robot(run_id) 无单列索引；无静态任务属性索引 |
| w8 `progress_summary` | Robot.id；AgentState 的机器人/时间窗；Step 的运行/时间窗 | Step.complete 另行过滤；统计需要处理窗口内的样本，无明显的主要索引缺口 |
| w9 `completion_feedback` | RuntimeEvent 的运行/机器人/时间窗 | event.type/task_id 非空另行过滤；无静态任务属性索引；结果聚合后排序 |
| w10 `recovery_markers` | 两处 RuntimeEvent 均可匹配运行/机器人/tick 索引 | type 另行过滤；按类型求最大 tick 仍需处理候选事件，不必据此立即再建 type 索引 |
| w11 `handoff_outcomes` | Handoff 的运行/时间窗；ExecutionState 的运行/任务/时间范围 | donor_id/receiver_id 的 OR、完成状态和次级排序另行处理；已有主要访问路径 |
| w12 `rack_workload` | AgentState 的机器人/tick；Location 通过任务关系访问 | Robot(run_id) 无单列索引；无 `WarehouseTask(dataset_id,task_id)`；货架聚合和排序仍需处理 |
| w13 `port_pressure_history` | Run 锚点和 Dataset 关系路径 | 缺 `DispatchState(run_id,dataset_id,tick)`；`commit_seq` 是次级排序字段，不能替代 tick |

复合索引不能直接按关系型数据库的“只匹配最左字段就总能使用”来判断。Neo4j 复合 RANGE 索引不保存任一索引属性缺失的节点，计划器需要能够保证结果完整。比如只有 `Robot.run_id` 的条件，不能直接视为已有 `(run_id,robot_id)` 完整覆盖；仅有 `DispatchState.run_id/dataset_id/tick` 条件，也不能把 `(run_id,commit_seq)` 当作相同访问路径。[Neo4j 官方复合索引说明](https://neo4j.com/docs/cypher-manual/5/indexes/performance-indexes/using-indexes/)

## 静态图谱和查询锚点

历史报告 `data/neo4j-run-copies/million-110902/20261110-105153-3096608/result.json` 的节点总数为 1196138，和用户贴出的运行节点总数一致。报告列出 14 种运行态标签，没有 `Dataset`、`WarehouseTask`、`Warehouse`、`Location`、`Rack`、`Goods`，关系清单也没有 `USES_DATASET`。这表明该次复制没有带入静态图谱，但不能单凭历史报告证明目标库后来没有补充静态数据。

`backend/robot_operation_context.py:463` 的 `_anchor_usable` 要求 w0 返回恰好一个带 id 的 Dataset，以及有效的完整 Step。若 Dataset 关系缺失，真实读取的 context 为 `unavailable`，w1～w13 会在执行前标记 `skipped/anchor_unavailable`，见该文件第 508、536 行。这与索引是否 ONLINE 是两个问题。

可在用户已登录的目标 cypher-shell 中先执行以下只读核查：

```cypher
MATCH (r:Run {id:'million-110902'})
OPTIONAL MATCH (r)-[:USES_DATASET]->(d:Dataset)
RETURN r.id AS run_id, r.latest_step AS latest_step,
       count(DISTINCT d) AS dataset_count, collect(DISTINCT d.id) AS dataset_ids;

MATCH (d:Dataset)
OPTIONAL MATCH (d)-[:INCLUDES_TASK]->(t:WarehouseTask)
RETURN d.id AS dataset_id, count(t) AS included_tasks;
```

第一条的 `dataset_count` 应为 1。为 0 时，补索引无法修复缺失关系；即使库里有独立 Dataset，也必须和该 Run 正确连接。

## 候选补充索引

以下只是按访问模式给出的候选语句，本次没有在数据库执行。应先确认静态数据和关联完整，再结合计划和数据量决定是否创建；例如历史报告只有 1 个 Handoff，通常没有必要优先给它增加多套索引。

优先评估调度快照和货物事件的组合：

```cypher
CREATE RANGE INDEX warehouse_context_dispatch_dataset_tick IF NOT EXISTS
FOR (n:DispatchState) ON (n.run_id,n.dataset_id,n.tick);

CREATE RANGE INDEX warehouse_context_event_cargo_tick IF NOT EXISTS
FOR (n:RuntimeEvent) ON (n.run_id,n.cargo_id,n.tick);
```

其他候选由实际慢查询决定，不能全部视为执行所必需：

| 候选属性组合 | 目的与适用条件 |
|---|---|
| `RuntimeEvent(run_id,task_id,tick)` | w2/w3/w6 增加时间范围定位；已有 `(run_id,task_id)` 可用，属于增强 |
| `Handoff(run_id,task_id,tick)` | w3 避免按时间找到候选后再过滤任务 |
| `Handoff(run_id,goods_id,tick)` | w7 避免按时间找到候选后再过滤货物 |
| `Robot(run_id)` | w7/w12 的按运行枚举；100 个机器人时通常不是主要成本 |
| `Dataset(id)` | 静态数据定位；从 Run 展开关系的计划未必需要它 |
| `WarehouseTask(task_id)` | 不修改当前 w2/w3 时，给它们的 task_id 条件增加属性访问路径；其他任务查询也可先按 task_id 查再验证 dataset |
| `WarehouseTask(dataset_id,task_id)` | 适配 w6/w7/w9/w12；项目已有静态 schema 定义但目标清单中没有 |

最后两项是应结合查询选择的方案，不建议不经评估同时新增。w2/w3 当前只在任务节点上约束 `task_id`，Dataset 作用域通过关系限定；若选择复合任务索引并希望 w2/w3 也利用它，应同时让这两条的任务模式显式约束 `dataset_id:$dataset_id`，并先确认现有任务数据满足该属性约定。本次没有修改这两条查询。

## 验证范围

已读取实际 14 条 Cypher、查询包选择、结果锚点校验和项目 schema 定义，并逐项对照用户提供的 32 项索引；解析目录确认默认 `core` 为 w0/w5。没有对目标库运行 EXPLAIN/PROFILE，因此没有宣称已验证实际索引命中率、DB Hits 或查询耗时。

确认计划应在目标库对带真实参数的查询使用 EXPLAIN；确认执行成本再针对单条读取查询使用 PROFILE。[官方 EXPLAIN/PROFILE 说明](https://neo4j.com/docs/cypher-manual/5/planning-and-tuning/execution-plans/)

当前脚本虽然支持 `--explain`，但 `execute_context` 只保存状态、返回行数和时间，没有保存 `summary.plan`；所以其 `status=ok` 只能证明规划未报错，不能证明每处访问使用了哪个索引。

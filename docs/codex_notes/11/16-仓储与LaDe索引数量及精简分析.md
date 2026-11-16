# 仓储与 LaDe 索引数量及精简分析

## 关键结论

如果范围仅是当前 w0～w13 的读取，仓储的整套索引确实有精简空间。之前“整体合理”的判断只说明这些索引有创建来源，没有充分区分完整系统用途和当前 w 查询的必要性。

但 32 对 3 不是相同口径：仓储的 32 项包含约束支撑索引和默认 LOOKUP；LaDe 源码里的 3 项只是 AgentState 的普通查询索引，源码另外声明了写入唯一约束。源码声明不能代替对 LaDe 实库的 SHOW INDEXES 核对。

## 数量从哪里来

| 类型 | 仓储：用户提供的实际清单及声明来源 | LaDe：本次核实的源码声明 |
|---|---|---|
| 普通查询 RANGE 索引 | 16 项：基础运行 schema 12 项，加 C 查询模块实际新增 4 项 | 3 项 AgentState 索引 |
| 唯一约束的支撑索引 | 14 项：运行实体标识约束 12 项，加检查点/提交序列约束 2 项 | 写入初始化声明 7 项；Neo4j 运行态模式另外声明 1 项 |
| 默认 LOOKUP | 2 项 | 新建库通常有 2 项；本次没有取得 LaDe 实库输出 |
| 合计 | 32 项，用户清单全部 ONLINE | 若上述定义全部安装且保留默认 LOOKUP，所列部分是 13 项；这不是对 LaDe 实库总数的断言 |

唯一约束创建的支撑索引也会显示为 RANGE，因此只看 `type=RANGE` 无法区分“普通查询索引”和“约束索引”；可查看 `owningConstraint`。[Neo4j 约束与支撑索引说明](https://neo4j.com/docs/cypher-manual/5/constraints/create-constraints/)

C 模块还声明 Run.id 和 Step(run_id,tick)，但它们与基础 schema 等价，不会把当前清单重复增加两项。对现有 30 项非 LOOKUP 定义按类型、标签和属性顺序检查，没有发现完全相同的两份定义。当前讨论的精简主要是用途重叠和当前任务用不到，不是完全相同的索引被反复建了多份。

## LaDe 的三条具体是什么

`/data/zc/datasets/LaDe/viewer/simulation/knowledge.py:8` 的 `AGENT_STATE_INDEXES`：

| 属性组合 | 主要访问方向 |
|---|---|
| `AgentState(run_id,drone_id,step_index)` | 单无人机当前/历史状态 |
| `AgentState(run_id,region_id,step_index)` | 区域状态 |
| `AgentState(run_id,step_index)` | 全局步状态 |

同文件第 343 行的 `ensure_writer_constraints()` 另外声明 Run、Drone、Task、Region、Event、Step、AgentState 的 7 项唯一约束；`simulation/knowledge_writer.py:73` 的 bootstrap 路径会调用它。第 293 行还声明 `DroneRuntimeState(run_id,execution_id,drone_id)` 约束。

LaDe 读取 i 系列时，Drone 等实体可以使用身份约束的支撑索引，不能把这部分贡献归到三条 AgentState 普通索引上。其他读取可以通过关系遍历或扫描完成；查询能返回正确结果，也不等于每条查询都获得了同样的索引优化或同样的时延。

## 仓储为什么普通索引更多

第一，仓储把运行历史拆成更多独立标签：AgentState、ExecutionState、OwnershipChange、Handoff、DispatchState、RunStat 等。索引按指定标签及属性定义，一条 AgentState 索引不能直接代替这些不同标签的时间查询索引。

第二，现有 schema 同时包含运行写入、全局统计、检查点恢复、仪表盘查询和 C/w 两套上下文入口，并不是从 w 系列的最小需求重新设计的清单。

第三，机器人标识有两套访问字段。C 查询使用 `robot_id`，当前 w 查询主要使用 `robot_node_id`，因此产生了相近用途的两组状态/事件索引。这是可以通过统一查询约定精简的设计成本。

这些原因解释数量，不证明现有每一项都值得保留，也不证明仓储必须维持当前 32 项。

## 有依据的精简候选

下表按“只评估当前 w 查询”列出候选，不是数据库删除清单。保留对应的旧入口或写入/恢复功能时，需要保留其访问能力，或者先调整这些调用方。

| 索引或组合 | 当前 w 使用情况 | 其他已找到的用途 |
|---|---|---|
| `query_c_state_robot_tick`：`AgentState(run_id,robot_id,tick)` | w 用 robot_node_id 和状态 id，没有相同的数字 robot_id 状态过滤 | `backend/warehouse_graph_query.py:27` 的 C self_state/recent_states，以及 fleet_states |
| `query_c_event_robot_tick`：`RuntimeEvent(run_id,robot_id,tick)` | w 机器人事件用 robot_node_id | 同文件第 34 行 C recent_events 的数字 robot_id 分支；另有 robot_node_id 分支 |
| `warehouse_runtime_state_run_tick`：`AgentState(run_id,tick)` | w 不直接按全场状态的运行/tick 条件读取 | 同文件第 309 行 replay_states，以及其他回放状态读取 |
| `warehouse_runtime_runstat_run_tick` | w 不查询 RunStat | 同文件 latest_complete_step 等读取 Step 与统计的关联 |
| `warehouse_runtime_dispatch_run_seq` | w1/w13 按 tick 查，不按 commit_seq 查 | 同文件 dispatch_state 按提交序列读取 |
| `warehouse_runtime_assist_run` | w 不查询 Assist；其 `(run_id,donor_id)` 组合对只查 run_id 的收益也需计划验证 | 全局 writer 和图谱接口仍访问 Assist，不能据此断言此具体组合必定被使用 |
| `warehouse_runtime_robot_run_tick` | w 不使用 last_written_tick 过滤 | 写者会更新该字段；本次未找到足以证明该复合索引收益的直接查询，是值得优先审查的候选 |
| 检查点/提交序列的两项约束 | w 不查询对应标签 | `backend/neo4j_live_store.py:28` 创建，服务提交去重与恢复状态；仍在使用此存储时不应按 w 的需求删除 |

对于只读实验副本，运行时不使用的查询索引可以单独评估；对于仍持续写入的完整系统，唯一约束还保护实体身份并支撑 MERGE，不能按“w 没用到”直接取消。

## 更合适的精简方向

1. 先统一 C/w 的机器人标识字段，减少两套历史查询组合；保留的字段和既有数据兼容性需要一起处理。
2. 将实体身份约束、当前任务的查询索引、可选统计/回放/恢复功能按实际启用范围管理；避免拿全系统清单作为每个只读实验的最低配置。
3. 针对 w1/w13 的 tick 条件、w7 的货物条件修正访问路径，同时评估旧组合的使用者。新增与精简应一起考虑，避免只追加索引而不清理过时访问约定。

本次没有足够的当前执行统计，不能给出“安全删除后必定剩几项”或“删完会快多少”的结论。索引会消耗存储，并在相关写入时增加维护成本；具体收益应比较代表性读取和写入的执行情况。[Neo4j 关于过度索引的说明](https://neo4j.com/docs/cypher-manual/current/indexes/performance-indexes/using-indexes/)

可用以下只读输出区分约束与普通索引，并观察使用统计：

```cypher
SHOW INDEXES
YIELD name,type,labelsOrTypes,properties,owningConstraint,readCount,lastRead
RETURN name,type,labelsOrTypes,properties,owningConstraint,readCount,lastRead
ORDER BY readCount,name;
```

readCount 为 0 不能单独作为删除依据：需要覆盖有代表性的查询和写入/恢复过程，也要考虑统计起点与进程重启。[官方 SHOW INDEXES 字段说明](https://neo4j.com/docs/cypher-manual/5/indexes/performance-indexes/list-indexes/)

## 本次核验

只读检查两边 schema、C/w/i 查询和仓储回放/统计/存储路径。通过 AST 解析 LaDe 三项普通索引和七项写入约束，并核对仓储基础 schema 的 12 项约束、12 项普通索引及 C 模块的净增 4 项。没有连接实库、删除索引、运行仿真或修改任何业务代码。

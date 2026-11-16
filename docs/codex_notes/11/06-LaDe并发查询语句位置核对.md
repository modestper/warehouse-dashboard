# LaDe 并发查询语句位置核对

### 任务背景

用户询问是否可以看到 LaDe 项目并发查询测试的实际查询语句。本次定位本机 `/data/zc/datasets/LaDe/viewer` 下的源码与少量已有日志，未连接数据库、未执行压测。总结保存在当前仓储工作区，未修改 LaDe 项目文件。

### 关键结论

- 可以直接读取 LaDe 原项目里的 Cypher 查询源码、参数构造、并发执行入口，不仅是查看查询耗时日志。
- 简单并发测试 `scripts/concurrent_neo4j_query.py` 内含6条 `PRESET_QUERIES`，包括机器人状态、机器人事件、机器人邻域、最近事件、运行节点统计、Step汇总；同时允许 `--cypher` 和 `--cypher-file` 自定义语句。
- 通用 Cypher 基准共有56条 `q0～q55`：基础13条在 `scripts/lade_cypher_benchmark.py`，扩展43条在 `scripts/lade_cypher_templates.py`。
- 机器人决策上下文查询也有56条 `i0～i55`，与q系列是不同语句集：基础12条在 `scripts/lade_llm_context_queries.py`，扩展44条在 `scripts/lade_llm_context_templates.py`。
- 上下文系列按自身状态、任务决策、能源安全、求助/助人、进度、恢复、记忆、区域态势和数据就绪等用途组织成查询包。查询包用于准备决策输入，不等于已经实现对应协助决策或协助关系。
- 简单并发和通用基准使用线程池派发查询；上下文基准并发执行各机器人的context任务，每个context里再执行选中的查询。因此“100并发”不必然表示100种不同的查询。
- 已有 `neo4j_concurrent_100_agents_jilin_ray001_clean.jsonl` 的已读取开头记录使用 `agent-state`，各请求的 `agent_id` 不同；本次没有据此断言所有历史测试都只用了同一种语句。

#### 主要源码位置

| 用途 | 文件与入口 |
|---|---|
| 6条简单预设 | `/data/zc/datasets/LaDe/viewer/scripts/concurrent_neo4j_query.py:18`，`PRESET_QUERIES` |
| 简单并发执行 | 同文件第253行 `run_benchmark()` |
| 通用基准基础q0～q12 | `/data/zc/datasets/LaDe/viewer/scripts/lade_cypher_benchmark.py:57`，`DEFAULT_TEMPLATES` |
| 通用基准扩展q13～q55 | `/data/zc/datasets/LaDe/viewer/scripts/lade_cypher_templates.py:4` |
| 通用基准并发执行 | `lade_cypher_benchmark.py:636`，`run_benchmark()` |
| 上下文基础i0～i11 | `/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:44`，`CONTEXT_QUERIES` |
| 上下文扩展i12～i55 | `/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:134` |
| 上下文查询包 | `lade_llm_context_queries.py:343`、`lade_llm_context_templates.py:696` |
| 单context和并发入口 | `lade_llm_context_queries.py:564` 的 `execute_context()`、第721行的 `run_context_benchmark()` |

#### 实际语句示例

简单预设 `agent-state` 原文：

```cypher
MATCH (d:Drone {id: $agent_id, run_id: $run_id})
RETURN d.id AS drone_id,
       properties(d) AS properties
LIMIT 1
```

它读取指定运行中某一机器人的当前属性。并发测试可以让不同请求使用不同的 `$agent_id`。

通用基准 `q3 / agent_state_trajectory` 原文：

```cypher
MATCH (a:AgentState {run_id: $run_id, drone_id: $drone_id})
WHERE a.step_index >= $start_step AND a.step_index <= $end_step
RETURN a.step_index AS step_index,
       a.time_minute AS time_minute,
       a.x AS x,
       a.y AS y,
       a.status AS status,
       a.queue_len AS queue_len,
       a.battery_remaining_m AS battery_remaining_m
ORDER BY a.step_index
```

它读取单个机器人在指定Step窗口内的轨迹和状态。上下文系列另外包含当前任务、历史状态、可用同伴、电量约束、任务进度等读取。

### 执行过的操作

- 使用 `rg --files` 定位 LaDe 项目和查询/压测源码，阅读其 `AGENTS.md`。
- 使用 `rg -n`、`sed -n` 定位查询字典、扩展模板、参数传递、线程池与执行入口。
- 用 `head` 只读查看已有简单并发日志开头，确认其预设名与参数字段。
- 使用当前工作区 `./run_in_lade.sh python -B -c '…'`，仅通过标准库 `ast` 解析源码，统计6条简单预设、13+43条q系列和12+44条i系列；没有导入执行压测模块。
- 未运行查询、压测、服务控制或数据库操作。

### 修改内容

- 仅新增本文档，没有修改 LaDe 或仓储业务代码、配置和历史结果。

### 后续建议

- 如需复现特定历史压测，应先根据其启动参数或日志中的查询ID确认使用简单预设、q系列还是i系列，再核对实际参数；不要把三个集合混为一套。
- 本次只读定位无需用户确认。

# Ray两次查询测试分析

第二次 `w1～w13` 的 `avg=N/Ams` 表示没有可统计的成功执行样本：这些语句全部被跳过。`w0` 有平均耗时7589.234ms。第一次的后13条也全部跳过，并非更换汇总显示格式后才出现问题。

## 两次测试对照

数据来源为 `data/warehouse_context_queries_ray/` 下两份完整的 `summary.json / queries.jsonl / contexts.jsonl`，逐行核对了两份各7000条查询记录和500份context。

| 指标 | 16:34测试 `20261111-163425-5df495d7` | 16:53测试 `20261111-165326-9f8d29b0` |
|---|---:|---:|
| runId / 数据库 | million-110902 / neo4j | 相同 |
| 机器人 / repeat / context | 100 / 5 / 500 | 相同 |
| 就绪Actor / 有结果的Actor | 100 / 100 | 100 / 100 |
| 实际尝试的查询 | 500，全部是w0 | 500，全部是w0 |
| w0 SQL执行成功 | 9 | 500 |
| w0 SQL执行失败 | 491，均为事务超时 | 0 |
| 成功形成可用上下文 | 0 | 0 |
| w1～w13跳过次数 | 6500 | 6500 |
| 跳过原因 | anchor_unavailable | anchor_unavailable |
| 总墙钟时间 | 262.332秒 | 51.402秒 |
| 查询阶段墙钟时间 | 249.439秒 | 38.542秒 |
| w0均值，仅成功样本 | 28.828秒，样本9条 | 7.589秒，样本500条 |
| w0均值，全部尝试 | 47.018秒 | 7.589秒 |

第一份摘要是较早输出结构，`queries.w0.mean_ms` 包含失败尝试耗时；第二份新增 `by_query.w0.avg_ms` 只统计成功查询。因此不能直接把第一次的47.018秒与第二次的7.589秒当作相同样本口径的比较。

## N/A的完整原因

第二次500份w0结果均显示：

```text
t = published_tick = 10000
step_count = 1
step.complete = true
step.expected_robot_count = step.received_robot_count = 100
self_state.tick = 10000
state_age_ticks = 0
datasets = []
observation_ready = false
```

时间步、本机状态和采样新鲜度都有值，未匹配到的是查询所需的：

```cypher
(run:Run)-[:USES_DATASET]->(dataset:Dataset)
```

读取器的 `_anchor_usable()` 要求恰好一个有效数据集。`datasets=[]` 使该条件失败，context标记为 `unavailable`，其余13条因统一时间/数据集前置条件不足而跳过。第二次500份context的状态全是 `unavailable`。

这也解释了“SQL成功500条，但整个批次仍为error”：SQL执行成功只是正确返回了一行；这一行没有提供后续业务读取所需的数据集关联。`w1～w13` 没有真正发送到Neo4j，不是执行耗时为0，也不是仅仅返回了0行。

统计函数只用成功执行样本求均值；空样本返回null，终端将null显示为N/A并拼接ms，所以看到 `N/Ams`。旧版第一份摘要中，同一情况表现为 `mean_ms: null`。

## 第一次的额外问题

491条错误均为：

```text
Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration
The transaction has not completed within the timeout specified at its start by the client.
```

剩余9条成功返回的w0也都是 `datasets=[]`，因此同样未形成可用上下文。第一次同时存在事务超时和数据集关联不满足两个问题；第二次没有SQL异常，但数据就绪问题依旧存在。

两个测试的元数据均记录100个独立Actor PID、100个不同物理核心，CPU affinity校验通过，Actor资源均为1 CPU。根据context的创建时间和耗时估算，重叠峰值均约100份context。日志支持“Ray客户端并发已启动”，不能据此认定数据库也有100个并行执行线程或100核资源。

## 能说明和不能说明的性能变化

第二次w0按repeat序号分组的平均耗时：

| repeat_index（从0开始） | w0成功平均耗时 |
|---|---:|
| 0 | 22.214秒 |
| 1 | 6.196秒 |
| 2 | 3.449秒 |
| 3 | 3.311秒 |
| 4 | 2.776秒 |

后续任务明显变快，符合缓存/查询计划预热或数据库负载变化的表现；repeat不是逐轮屏障，任务可能重叠。仅凭这批客户端日志，不能确定具体是哪一类缓存、索引或资源因素，也不能把变化归因于终端格式修改。

第二次的9.727qps是500次w0尝试除以含Ray启动和清理的51.402秒，不是14条业务查询都正常运行后的吞吐。基础w0仍达到秒级，后续应结合实际索引、执行计划和Neo4j资源配置排查。

另有75份样本包含task_id，涉及15台机器人；即便已有任务，相关查询也被更早的数据集前置条件拦住。425份无当前task_id不等于没有待办任务，因为候选池w1根本没有执行。本批日志不足以评估任务、货物、接力、货架和工位等查询性能。

## 数据集为什么没匹配到

现有日志不能区分“静态Dataset没导入”“Run没有绑定正确Dataset”“节点标签/关系或副本范围不符合查询约定”。共享静态Dataset及WarehouseTask默认不属于按run_id复制的运行子图，但不能直接断定是复制丢边：历史 `10-million-110902-source-plan.json` 本就记录 `cross_boundary_relationships=[]`，源Run当时也可能没有绑定。

`backend/global_writer.py` 只有在指定Dataset节点已经存在时才建立 `USES_DATASET`，不会自动创建完整静态数据集。因此需要核对实际目标库中的Run声明、Dataset和关联，再决定导入静态数据还是补齐正确关联。不要仅创建一个空Dataset节点来绕过校验，后续查询还需要WarehouseTask、Goods、Location和布局等事实。

可先在目标库执行以下只读核对：

```cypher
MATCH (r:Run {id:'million-110902'})
OPTIONAL MATCH (r)-[:USES_DATASET]->(d:Dataset)
RETURN r.id AS run_id, r.dataset_id AS declared_dataset_id,
       r.latest_step AS latest_step, collect(d.id) AS linked_datasets;
```

```cypher
MATCH (d:Dataset)
RETURN d.id AS dataset_id, d.start AS start
LIMIT 20;
```

补齐并验证匹配的静态数据及Run关联后，先用1台机器人、repeat=1验证哪些语句实际执行，再进行100并发测试。无当前/待办任务时，任务型查询仍可能按设计跳过，应选择有活动任务的已发布tick或明确提供合法taskId。

证据：[第一次摘要](../../../data/warehouse_context_queries_ray/20261111-163425-5df495d7/summary.json)、[第一次查询日志](../../../data/warehouse_context_queries_ray/20261111-163425-5df495d7/queries.jsonl)、[第二次摘要](../../../data/warehouse_context_queries_ray/20261111-165326-9f8d29b0/summary.json)、[第二次context结果](../../../data/warehouse_context_queries_ray/20261111-165326-9f8d29b0/contexts.jsonl)、[读取器](../../../backend/robot_operation_context.py)、[汇总统计](../../../scripts/warehouse_llm_context_queries_ray.py)。本次结论来自保存日志和源码核对，没有连接实际数据库确认节点/索引/资源现状，没有更改查询代码、原始日志或图谱数据。

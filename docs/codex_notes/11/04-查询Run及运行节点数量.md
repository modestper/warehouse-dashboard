### 任务背景

用户希望在Neo4j中查看现有哪些Run，以及每个Run所属节点的数量。

### 关键结论

- 本项目运行根节点是 `:Run`，运行编号存储在 `id` 属性。
- 运行态机器人、状态、事件等节点通过 `run_id` 标识归属；按该属性统计比只统计Run直接邻居更完整。
- 以下统计排除共享的Dataset、WarehouseTask、地图等静态节点；加1后包含Run根节点自身。
- 应连接业务数据库（当前配置为neo4j），而不是system管理库。

列出Run：

```cypher
MATCH (r:Run)
RETURN r.id AS run_id, r.dataset_id AS dataset_id,
       r.latest_step AS latest_step
ORDER BY run_id;
```

每个Run的运行态节点数量：

```cypher
MATCH (r:Run)
OPTIONAL MATCH (n)
WHERE n.run_id = r.id
RETURN r.id AS run_id,
       count(n) AS runtime_nodes,
       count(n) + 1 AS nodes_including_run
ORDER BY runtime_nodes DESC;
```

指定Run按标签组合统计：

```cypher
MATCH (n)
WHERE n.run_id = 'test_1104'
RETURN labels(n) AS node_labels, count(*) AS node_count
ORDER BY node_count DESC;
```

统计Run数量：

```cypher
MATCH (r:Run) RETURN count(r) AS run_count;
```

### 执行过的操作

只读核对 `backend/warehouse_runtime_graph.py` 和 `backend/neo4j_live_store.py` 中Run写入、运行编号及运行态节点归属字段；没有连接或修改实际数据库。

### 修改内容

新增本文，保存可直接执行的查询及统计口径。

### 后续建议

在业务库执行查询；将示例中的test_1104替换为实际Run编号。若想统计共享的静态输入，需要另按Run关联的Dataset统计，避免通过无界路径遍历混入其他运行。

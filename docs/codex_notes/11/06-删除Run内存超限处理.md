### 任务背景

删除6912_fast_1104时，按5000节点一批执行DETACH DELETE触发Neo4j内存池限额。需要缩小实际事务工作量，而不是仅按节点数估算。

### 关键结论

- DETACH DELETE同时删除节点及其关联关系，5000个节点不代表仅处理5000条记录。
- 项目的Robot连接AgentState历史，Run连接多种运行节点，检查点连接提交记录；删除高连接度节点可能产生较大的事务。
- 简短错误不足以确定具体配置项、单事务限额或全局事务内存压力，不直接调整内存配置。
- 改为先按关系分批删除，再按节点分批删除；先停止目标仿真，避免并发重新写入关系。
- 分批删除已成功提交的批次不会因后续批次失败而整体撤销，以下命令可继续处理剩余数据。

先停止目标运行，在业务数据库neo4j的cypher-shell中逐条执行。

第一步，删除与该运行节点相连的关系，每批500条。按有向关系扫描，每条关系仅匹配一次，不使用会收集全量结果的排序或去重。

```cypher
MATCH (a)-[r]->(b)
WHERE a.run_id = '6912_fast_1104'
   OR b.run_id = '6912_fast_1104'
   OR (a:Run AND a.id = '6912_fast_1104')
   OR (b:Run AND b.id = '6912_fast_1104')
CALL {
  WITH r
  DELETE r
} IN TRANSACTIONS OF 500 ROWS;
```

第二步，删除所属运行节点及根节点，每批100个。普通DELETE会在仍有关系时拒绝删除，避免再次隐式扩展大量关系。

```cypher
MATCH (n)
WHERE n.run_id = '6912_fast_1104'
   OR (n:Run AND n.id = '6912_fast_1104')
CALL {
  WITH n
  DELETE n
} IN TRANSACTIONS OF 100 ROWS;
```

核查剩余数量：

```cypher
MATCH (n)
WHERE n.run_id = '6912_fast_1104'
   OR (n:Run AND n.id = '6912_fast_1104')
RETURN count(n) AS remaining_nodes;
```

关系步骤会扫描数据库关系，数据量大时可能耗时，但不删除其他运行或共享静态节点。两步之间不可重新启动目标运行。Browser执行每条分批语句时加:auto；cypher-shell直接执行，不先:begin。

### 执行过的操作

读取backend/warehouse_runtime_graph.py和backend/neo4j_live_store.py，核对状态、机器人、Run和检查点关系；核对Neo4j官方删除文档：https://neo4j.com/docs/cypher-manual/5/clauses/delete/ 。没有连接或修改实际数据库，未改变Docker内存设置。

### 修改内容

新增本文，记录更小批次、先关系后节点的处理命令；在此前删除说明中加入内存超限的指引。

### 后续建议

若仍出现内存限额错误，保留完整错误或Neo4j日志，尤其是内存数值和具体setting名称，再判断是否存在全局内存争用或需要调整实例配置。不能仅凭当前错误确定根因。

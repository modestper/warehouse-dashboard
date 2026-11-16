### 任务背景

用户询问如何删除指定runId在Neo4j中的全部运行节点。本次仅提供命令，没有执行删除。

### 关键结论

如按节点分批删除出现内存超限，改用[先分批删除关系，再删除节点](06-删除Run内存超限处理.md)，避免高连接度节点的一次隐式关系删除。

- 当前项目运行节点通过run_id归属，Run根节点通过id定位。
- 先停止对应仿真进程，防止删除过程中继续写入。
- 分批删除运行节点，再删除Run根节点；共享地图、Dataset、静态任务与货物保留。
- 分批事务逐批提交，执行中断后可重新运行命令处理剩余节点。
- 数据库删除不会清理本地运行目录；新实验建议采用新runId。

在业务数据库neo4j中执行，将test_1104替换为目标编号。

删除前统计：

```cypher
MATCH (n)
WHERE n.run_id = 'test_1104'
   OR (n:Run AND n.id = 'test_1104')
RETURN count(n) AS node_count;
```

分批删除所属运行节点及它们的关系：

```cypher
MATCH (n)
WHERE n.run_id = 'test_1104'
CALL {
  WITH n
  DETACH DELETE n
} IN TRANSACTIONS OF 5000 ROWS;
```

随后删除根节点：

```cypher
MATCH (r:Run {id: 'test_1104'})
DETACH DELETE r;
```

再次执行删除前的统计，结果应为0。

cypher-shell默认以隐式事务执行，不要先执行:begin。Neo4j Browser执行分批删除语句时，应在该语句前加:auto。

### 执行过的操作

只读核对backend/warehouse_runtime_graph.py、backend/neo4j_live_store.py和backend/warehouse_knowledge_writer.py中的Run、运行态、检查点及提交记录字段。

### 修改内容

新增本文，保存删除命令和统计范围。

### 后续建议

确认数据库和目标runId后自行执行上述删除；该操作会永久删除该运行的数据库记录。本次未操作实际数据库。

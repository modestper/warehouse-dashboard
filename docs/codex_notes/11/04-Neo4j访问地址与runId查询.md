# Neo4j 访问地址与 runId 查询

### 任务背景

用户希望查询 Neo4j 中已有的 runId，并了解为什么服务器 IP 的 47474 端口无法访问，而 17474 可以访问。

### 关键结论

- 本次实际检查显示，HTTP 47474 和 Bolt 47687 都只监听 `127.0.0.1`，不能通过 `222.20.99.83` 直接访问。
- HTTP 17474 和 Bolt 17687 监听 `0.0.0.0` 及 IPv6 通配地址，允许通过服务器网络地址连接；用户已确认 17474 可以访问。
- 两组端口不能视为同一数据库；本次未验证它们的数据库内容，也未确认当前仿真服务实际使用哪个实例。
- 当前运行图谱代码使用 `Run.id` 存储运行编号；Neo4j 无 journal 检查点使用 `WarehouseLiveCheckpoint.run_id`。旧图谱另有 `WHRun.run_id`。
- 数据库查询尚未成功执行，不能据此列出真实已有的 runId。未修改端口配置或数据库。

### 执行过的操作

- 阅读 `AGENTS.md`、`ops/warehouse_kg_config.py`、`backend/warehouse_runtime_graph.py`、`backend/neo4j_live_store.py`、`docs/NEO4J_ONLY_START.md` 及当日相关总结。
- 使用 `rg`、`sed` 核对运行节点和连接配置。
- 执行 `ss -ltnp`，随后用 `ss -ltn '( sport = :47474 or sport = :17474 or sport = :47687 or sport = :17687 )'` 复核监听地址。
- 尝试通过 `./run_in_lade.sh python` 读取本机服务和连接配置，沙箱访问受限；后续数据库只读检查授权请求被用户中断，没有获得数据库查询结果。

### 修改内容

- 仅新增本文，记录访问差异与查询方法。

### 后续建议

在自己电脑上通过 SSH 转发访问仅绑定服务器本机的实例（替换 SSH 用户名）：

```bash
ssh -N -L 47474:127.0.0.1:47474 -L 47687:127.0.0.1:47687 用户名@222.20.99.83
```

保持 SSH 连接，打开 `http://127.0.0.1:47474/browser/`，Browser 的连接地址填写 `bolt://127.0.0.1:47687`。如果本机这两个端口已占用，可改转发命令左侧端口，并相应调整浏览器和 Bolt 地址。

连接正确的实例和业务数据库后，列出运行编号：

```cypher
MATCH (r:Run)
RETURN r.id AS run_id, r.dataset_id AS dataset_id,
       r.latest_step AS latest_step
ORDER BY r.started_at_ms DESC;
```

查看检查点中的运行编号：

```cypher
MATCH (h:WarehouseLiveCheckpoint)
RETURN h.run_id AS run_id, h.seq AS commit_seq, h.tick AS tick
ORDER BY run_id;
```

查询旧版图谱时使用 `MATCH (r:WHRun) RETURN r.run_id AS run_id;`。选择库名以实际服务配置为准，不能仅凭仓库默认值确定。

本次无需用户确认；若后续要修改端口映射或重启实例，应先确认目标实例及现有运行影响。

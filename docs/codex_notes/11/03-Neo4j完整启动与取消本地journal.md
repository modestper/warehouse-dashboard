### 任务背景

用户要求提供完整的 Neo4j 仿真启动代码，并取消新运行的本地 SQLite journal。默认目标数据库为 warehouse，密码由运行者输入；本次不启动真实数据库、仿真或导入用户数据。

### 关键结论

- 原实时入口无论是否连接 Neo4j，都会创建 JournalKnowledgeStore；仅改启动参数无法取消 journal，因此新增了显式 --no-journal 模式。
- 新 Neo4jLiveStore 在一个 Neo4j 事务中写事件/增量批次与最新完整检查点，不创建 SQLite 文件；失败会报错，水位不前进，也不会回退到本地 journal。
- Robot/AgentState/RuntimeEvent 等查询图谱仍由有状态 Ray actor 写，全局 Step/统计/交接由独立 HTTP 服务写。
- 完整启动器为 ops/start_neo4j_live.py，默认 warehouse、medium1728、页面14583、全局服务14284、私有本地 Ray 集群；先检查连接，再导入静态数据并启动服务。
- 本地仍保存页面快照、JSON 检查点、结果与诊断日志。新模式目前仅支持新运行，明确拒绝 resume-run-id、旧 A 图谱与基于 journal 的查询摘要。
- 47 项相关测试通过；最后调整提交水位后，对存储/CLI 的9项测试再次通过。HTTP 8项测试在沙箱内阻塞，沙箱外使用模拟对象运行全部通过，未接触真实数据库。
- 未修改用户 Neo4j、容器、旧实验数据或已经运行的服务；实库与真实 Ray 的端到端启动尚未执行。

### 执行过的操作

- 读取当前 AGENTS.md、存储类 backend/knowledge.py、实时入口和运行时、全局服务、Ray 组合、相关测试和启动脚本。
- 运行 git status --short、git diff --check、git diff、启动器 --help。
- 第一组：`./run_in_lade.sh python -B -m unittest -v tests.test_neo4j_live_store tests.test_live_native_cli tests.test_warehouse_live_runtime tests.test_live_activation_config`，18项通过。
- 启动器模块4项测试通过，覆盖参数、check-only无副作用、服务退出与失败停止。
- `./run_in_lade.sh python -B -m unittest -v tests.test_robot_graph_async`，17项通过。
- HTTP回归在沙箱内停在 AnyIO/Starlette TestClient.__enter__，中断后用15秒诊断定时器确认阻塞位置；获准在沙箱外重跑 tests.test_warehouse_live_http，8项全部通过。
- 最终对提交状态作语义修正后，重跑 tests.test_neo4j_live_store、tests.test_live_native_cli，9项通过。
- 所有数据库写入测试使用事务驱动替身；live 单步测试使用真实仿真/控制/保存逻辑，替换 actor 执行和 Neo4j 传输，确认输出没有 journal.sqlite 文件。

### 修改内容

| 文件 | 内容 |
|---|---|
| backend/neo4j_live_store.py | 新增直接 Neo4j 提交存储：唯一约束、事务锁、连续序号、内容校验、幂等重复提交、失败不推进水位 |
| ops/warehouse_live.py | 新增 --no-journal 参数，校验所需新图谱路径，拒绝不兼容模式 |
| ops/warehouse_live_runtime.py | 选择直写存储或原 journal，运行身份和页面标记 local_journal=false |
| ops/start_neo4j_live.py | 完整启动器：凭证检查、静态导入、全局服务、私有 Ray/live、进程监控和退出顺序 |
| tests/test_neo4j_live_store.py | 新增事务回滚、幂等冲突、损坏校验和真实 live 单步无 SQLite 测试 |
| tests/test_start_neo4j_live.py | 新增启动器参数、检查模式和子进程管理测试 |
| tests/test_live_native_cli.py | 验证新参数与不完整配置拒绝 |
| docs/NEO4J_ONLY_START.md | 完整命令、参数、数据去向、实库验证及恢复边界 |
| README.md | 增加无 journal 启动入口 |

Neo4j新增归档节点：

- WarehouseLiveCommit：每个主进程提交点的增量批次与全部新事件；run_id、seq唯一。
- WarehouseLiveCheckpoint：每个run的最新完整检查点；run_id唯一。
- 事件批次与检查点在同一事务内提交；head先获得写锁，再校验连续序号和历史摘要。重复提交校验检查点及事件内容，不一致则拒绝。
- durable_seq/direct_commit_seq 表示这些直接提交的水位，mirrored_seq保持0以免误启用旧A套查询；actor每步确认仍使用Run.latest_step。
- 原运行清理器按run_id还有通用节点扫描，可涵盖新节点；本次未执行清理。

### 后续建议

在项目根目录执行：

```bash
./run_in_lade.sh python ops/start_neo4j_live.py --database warehouse
```

输入真实数据库密码后，启动器会首先检查 warehouse 是否存在及凭证是否有效，随后导入 medium 静态图谱并启动新仿真。默认页面 http://127.0.0.1:14583/。

仅检查连接可以加 --check-only；已导入静态数据可以加 --skip-import；快速功能核对可用 --dataset-id medium19。输入数据与地图必须匹配。

本次验证不替代真实 Neo4j/Cypher 与100个Ray actor的端到端验收。新模式暂不支持从图谱断点恢复；这些限制已写入操作说明，不需要用户确认即可阅读和执行代码。

### 任务背景

用户要求参照本机 LaDe 项目，通过 KG_BACKEND、KG_RUN_ID、KG_WRITE_MODE、KG_STATE_SOURCE、KG_RECORD_AGENT_STATE 和 NEO4J_* 环境变量启动仓储仿真，并明确要求迁移 main、ray、process 三种写入模式。沿用之前不生成本地 SQLite journal 的要求。

### 关键结论

- 已读取 `/data/zc/datasets/LaDe/viewer/simulation/knowledge_writer.py`、`knowledge_state.py` 和动态入口，参考其“写入路由、写进程独立创建驱动、纯数据跨进程、回执确认、完整状态独立于历史”的实现。
- 新增统一连接配置，优先级为命令行 > NEO4J_* > WAREHOUSE_NEO4J_* > 默认值；连接、静态导入、运行态writer、全局writer和查询使用一致目标。
- main 在仿真主进程写运行态图谱、全局事实和归档；process 在专用spawn子进程写；ray 由每台机器人actor写自身状态、专用writer actor写全局事实与归档。静态导入是启动前独立步骤。
- 三种新模式使用同步确认，收齐100台机器人成功回执后才发布完整Step；不声称复制了LaDe的队列批处理吞吐。旧异步HTTP图谱路径仍保留，不能与新kg-write-mode混用。
- KG_STATE_SOURCE=neo4j 确实触发完整Step后的图谱读取；ray映射到内存返回状态。KG_RECORD_AGENT_STATE=1 默认逐步保存AgentState，0关闭该历史但保留当前完整状态和事件。
- KG_RUN_ID 成为真实runId；启动器拒绝已有输出/图谱runId，默认不覆盖旧结果。新入口仍不创建journal.sqlite，仅保留页面/JSON/诊断文件，暂不支持断点恢复。
- 配置、保存回滚、图谱、启动器、控制器及HTTP定向测试通过；真实子进程和真实Ray writer actor均已验证写入PID。尚未连接用户的真实Neo4j进行端到端写图。

### 执行过的操作

- 只读参考LaDe的 `simulation/knowledge_writer.py`、`simulation/knowledge_state.py`、`scripts/run_dynamic_sim.py`，未修改LaDe。
- 检查并修改仓储启动器、live入口与运行时、Ray机器人、运行态writer、全局服务和静态导入入口。
- 第一组26项：test_neo4j_live_store、test_live_native_cli、test_robot_graph_async，通过。
- 配置/启动器/路由/运行态图谱第一轮22项，其中真实Ray门控项跳过，其余通过。
- 真实仿真控制补测：main/process × memory/neo4j四种组合均通过，覆盖单步、实际主/子进程PID、状态读取触发及无SQLite文件；Neo4j传输与机器人池使用替身。
- 原有live状态控制、暂停/恢复、失败现场保护、激活配置共9项通过。
- 全局事实、medium布局与CSV来源回归13项通过。
- HTTP回归8项在沙箱外通过，使用模拟运行对象，不连接数据库。
- 真实Ray writer门控测试在沙箱内因GCS无法启动失败；获准在沙箱外以私有local集群重跑，一项通过，验证专用actor PID不同于主进程，并执行ray.shutdown。
- 最终对统一连接策略复核后运行 config、launcher、writer、async 四模块32项：31通过、1真实Ray门控跳过；该门控项已在独立真实测试中通过。
- 执行启动器、live和导入脚本的 --help 与 git diff --check，通过。未安装依赖、修改容器、重启用户服务或执行真实静态导入。

### 修改内容

| 文件 | 修改目的 |
|---|---|
| ops/warehouse_kg_config.py | 新增共享环境解析、连接配置、双名称规范化、严格开关校验；密码repr隐藏 |
| backend/warehouse_knowledge_writer.py | main/ray/process真实写入路由；驱动只在所属进程建立；进程/Ray超时及回执；共享归档接口 |
| ops/start_neo4j_live.py | 支持LaDe风格环境变量、runId、状态/历史选项与本地Ray CPU参数；按所选模式启动，无需额外HTTP全局服务 |
| ops/warehouse_live.py | 新增run-id、kg-write-mode、record-agent-state、Neo4j目标与Ray CPU参数，并接入环境默认值 |
| ops/warehouse_live_runtime.py | 路由对象注入仿真与归档，身份/页面状态记录真实mode；保留旧journal和旧HTTP模式 |
| backend/robot_actor/ray_actor.py | 主/子进程写状态或actor写状态，统一成功回执后发布Step；当前完整状态读图逻辑保持 |
| backend/ray_medium/sim.py | 使用共享连接配置；将schema/Run准备交给所选writer，传递历史开关 |
| backend/warehouse/composition.py | 本地Ray CPU预算传入物理执行池 |
| backend/warehouse_runtime_graph.py | 历史AgentState开关，当前Robot完整状态及事件继续写入 |
| backend/neo4j_live_store.py | 归档连接采用共享解析策略 |
| backend/global_writer_api.py | 独立旧HTTP入口同样接受通用NEO4J_*连接变量 |
| scripts/import_warehouse_neo4j.py | 统一通用变量、warehouse别名与CLI优先级 |
| tests/test_warehouse_kg_config.py | 配置优先级、密码隐藏、严格状态/历史值校验 |
| tests/test_warehouse_knowledge_writer.py | 写入PID、通道选择、Step屏障、失败传播、真实live单步及门控真实Ray测试 |
| tests/test_start_neo4j_live.py | 环境参数实际转发、CLI覆盖、目标一致性、重复runId拒绝 |
| tests/test_warehouse_runtime_graph.py | 关闭历史仍更新当前状态并记录真实事件 |
| docs/NEO4J_ONLY_START.md、README.md | 更新完整环境启动命令与三模式语义 |

本次保留之前未提交的代码和文档。三模式采用同一C套业务图谱，不另造Task/Drone配送模型；机器人数量仍由仓库配置决定，当前medium为100台。

完整命令示例（密码未设置时交互输入）：

```bash
KG_BACKEND=neo4j \
KG_RUN_ID=warehouse-demo-001 \
NEO4J_URI=bolt://127.0.0.1:47687 \
NEO4J_USER=neo4j \
NEO4J_DATABASE=warehouse \
KG_WRITE_MODE=ray \
KG_STATE_SOURCE=neo4j \
KG_RECORD_AGENT_STATE=1 \
./run_in_lade.sh python ops/start_neo4j_live.py \
  --host 127.0.0.1 --port 14583 \
  --backend ray --ray-address local --ray-num-cpus 5 \
  --dataset-id medium19
```

KG_WRITE_MODE可改为main或process。每次新运行使用新KG_RUN_ID；目标库必须真实存在，NEO4J_URI对应实际Bolt映射端口。数据库实例与数据库名称是两个参数，不能只改URI就假定库名也改变。

### 后续建议

- 使用新启动器的 --check-only 先验证实际数据库凭证/库名，再运行短的medium19样例。
- 需要无人值守时通过NEO4J_PASSWORD注入凭证；本项目不将密码写入运行manifest或文档。
- 再用medium1728执行完整回归；主进程串行写、独立子进程写与分散Ray写的性能需要在同一真实Neo4j上分别测量。
- 本次所有实库操作留待用户执行启动命令；代码修改和测试完成，无额外待确认事项。

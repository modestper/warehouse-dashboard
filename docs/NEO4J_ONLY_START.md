# Neo4j 完整启动，不创建本地 SQLite journal

如果遇到 `Graph not found: warehouse`，先按 [数据库不存在修复说明](NEO4J_DATABASE_NOT_FOUND.md) 检查实际库名及首次初始化方式。

在项目根目录运行：

```bash
./run_in_lade.sh python ops/start_neo4j_live.py --database warehouse
```

脚本默认连接 `bolt://127.0.0.1:47687`，用户名 `neo4j`，数据库 `warehouse`。兼容 LaDe 的 `NEO4J_*` 和 `KG_*` 环境变量；密码没有配置时交互输入，不回显、不写入文件。目标数据库必须已经存在，脚本不会把现有数据库改名。

## 与 LaDe 相同风格的启动参数

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

密码可以与 LaDe 一样通过 `NEO4J_PASSWORD` 传入；上述示例没有设置密码时会提示输入。每个新运行使用新的 KG_RUN_ID，不覆盖旧输出或已有图谱 run。

`NEO4J_URI` 决定数据库实例地址，`NEO4J_DATABASE` 决定该实例中的库名。例如实际 Bolt 映射为17687时，设置 `NEO4J_URI=bolt://127.0.0.1:17687`。不会自动探测其他端口或回退到另一数据库。

| 参数 | 实际行为 |
|---|---|
| `KG_BACKEND=neo4j` | 使用 Neo4j，不创建本地 SQLite journal；本启动器不支持纯内存后端 |
| `KG_RUN_ID` | 初始运行的真实 runId；未设置时自动生成 |
| `KG_WRITE_MODE=main/ray/process` | 决定运行态写入主体，见下表 |
| `KG_STATE_SOURCE=neo4j` | 同步确认完整 Step 后从图谱读机器人状态；读失败停止，不回退内存 |
| `KG_STATE_SOURCE=ray` | 使用 Ray actor 的内存返回状态，内部记作 memory；仍然写图谱 |
| `KG_RECORD_AGENT_STATE=1` | 默认每步记录 AgentState；显式 --graph-state-interval 可改采样间隔 |
| `KG_RECORD_AGENT_STATE=0` | 关闭 AgentState 历史，完整当前状态、事件与 Step 仍写入 |

运行态三种写入模式：

| 模式 | 机器人状态/历史/事件 | 全局 Step/交接/统计 | 检查点与提交批次 |
|---|---|---|---|
| main | 仿真主进程 | 仿真主进程 | 仿真主进程 |
| process | 独立 writer 子进程 | 同一个 writer 子进程 | 同一个 writer 子进程 |
| ray | 每个机器人自己的 Ray actor | 专用 writer Ray actor | 同一个 writer Ray actor |

默认写入模式为 main。三种模式的机器人运动仍由 Ray 执行，均保持同步写入回执和完整 Step 屏障。process 使用 spawn 子进程和一次一个请求的有界通信；ray 使用可确认的 actor 调用。这里迁移了 LaDe 的写入主体、独立驱动和错误传播方式，未承诺与 LaDe 的队列/批处理吞吐相同。

连接配置优先级为：显式命令行参数 > `NEO4J_*` > `WAREHOUSE_NEO4J_*` > 默认值。启动器统一解析一次，并将同一套值传给静态导入、仿真、writer 和查询进程，避免它们连向不同数据库。密码不写入运行说明和页面元数据。

`--ray-num-cpus 5` 为新建本地 Ray 集群设置5个CPU；默认将100个机器人 actor 的CPU额度调整到每个0.05，避免占用额度超过集群容量。`--ray-actor-cpus` 可以显式覆盖。连接已有Ray head时不允许指定 ray-num-cpus 改变其资源。

仓储输入通过 `--dataset-id` 选择，当前已注册全部9套：`medium19`、`medium1728`、`sample19`、`peak1728`、`day`、`peak-2h`、`hotspot-2h`、`continuous-2d`、`seven-patterns`。默认仍为 `medium1728`，机器人为地图配置的100台；地图、移动模型、配置和扰动由注册项自动选择，不把旧输入强行放到medium地图上。完整对应关系见[数据集注册与启动](REGISTERED_DATASETS.md)。LaDe的城市、日期、drone-count等配送参数不适用于仓库。

离线查看可用输入，不连接数据库、不启动服务：

```bash
./run_in_lade.sh python ops/start_neo4j_live.py --list-datasets
```

默认注册表为 `config/live-datasets.json`；可通过 `--registry` 显式选择其他注册表，保留的 `config/live-wheeled-datasets.json` 提供两个medium选项。

完整启动代码在 [ops/start_neo4j_live.py](../ops/start_neo4j_live.py)。脚本依次执行：

1. 检查所选数据包、依赖、凭证及指定数据库连接，检查页面端口空闲。
2. 导入所选数据包及其配对地图、位置表，使用该注册项独立的静态图谱标识。
3. 启动100个有状态Ray机器人及所选写入模式，默认页面 `http://127.0.0.1:14583/`，数据集为 `medium1728`。全局写入归所选模式所有，不另启14284 HTTP服务。

默认 Ray 地址 `local` 会由新的仿真进程创建独立 Ray 集群，不要求 `.venv/bin/ray` 或预先启动 Ray head。现有集群可以用 `--ray-address 127.0.0.1:26380` 指定。Ctrl+C停止本次仿真并关闭其writer，不停止数据库或其他运行中的服务。图谱写入长尾可能使退出等待较久。

可选参数示例：

```bash
# 只验证环境与数据库，不导入、不启动服务
./run_in_lade.sh python ops/start_neo4j_live.py --database warehouse --check-only

# 快速 19 任务样例；仍使用与其匹配的静态数据
./run_in_lade.sh python ops/start_neo4j_live.py --database warehouse --dataset-id medium19

# 静态数据已经导入时，跳过导入
./run_in_lade.sh python ops/start_neo4j_live.py --database warehouse --skip-import

# 指定页面、写入模式与倍速
./run_in_lade.sh python ops/start_neo4j_live.py --database warehouse \
  --port 14584 --kg-write-mode process --speed 10
```

依赖不足时，可在准备好环境后安装项目依赖：

```bash
./run_in_lade.sh python -m pip install -r requirements.txt -r requirements-live.txt
```

## 新增的无 journal 模式

原 live 命令支持显式 `--no-journal`。启动器实际组合为：

```bash
./run_in_lade.sh python -m backend.warehouse live \
  --registry config/live-wheeled-datasets.json \
  --dataset-id medium1728 --output-root data/neo4j-runs \
  --mode rules --knowledge-backend neo4j --no-journal \
  --mobility wheeled --robot-processes ray --ray-address local \
  --ray-actor-cpus 0.1 --robot-graph \
  --kg-write-mode main \
  --kg-state-source memory --graph-state-interval 4 \
  --autostart --initial-speed 1 --port 14583
```

直接使用这条命令，需要预先设置连接环境变量并完成静态导入。使用完整启动器可自动处理这些步骤。原无 kg-write-mode 的 HTTP 全局服务路径仍保留，不能与新的三种写入模式混用。

无 journal 模式的数据去向：

| 数据 | 保存位置 |
|---|---|
| 地图、货架、位置、固定任务、货物 | Neo4j 静态图谱 |
| 机器人当前完整状态、采样历史、运行事件 | Neo4j，由所选模式的writer写入 |
| 全局统计、交接、完整 Step | Neo4j，由所选模式的writer写入 |
| 每次保存点的增量批次与全部待提交事件 | Neo4j `WarehouseLiveCommit` |
| 最新完整提交检查点 | Neo4j `WarehouseLiveCheckpoint` |
| SQLite journal | 不创建、不写入 |
| 页面快照、运行说明、JSON 检查点、结果与诊断日志 | `data/neo4j-runs/<runId>/`，供查看和诊断 |

检查点和该保存点的事件批次在一个 Neo4j 事务内提交，数据库失败则该次提交报错，不会退回本地 journal。重复提交必须保持内容一致，提交序号必须连续。

`knowledge.durable_seq/direct_commit_seq` 在此模式指主进程检查点提交水位；旧 A 套的 `mirrored_seq` 保持 0，旧查询入口不启用。actor 的每步完整进度仍由 `Run.latest_step` 和页面 robotGraph 信息反映，不能把两个序号混用。

当前 `--no-journal` 仅支持新运行，不能搭配 `--resume-run-id`、旧 A 套图谱或基于本地 journal 的查询摘要。图谱检查点已存储，但这里不宣称完成了从图谱恢复全部 Ray/全局服务现场的验收。原 journal 模式不受影响。

## 核对实际写入

在 Neo4j Browser 选择 `warehouse` 后执行：

```cypher
MATCH (r:Run)
RETURN r.id AS run_id, r.latest_step AS latest_step
ORDER BY r.started_at_ms DESC LIMIT 5;
```

```cypher
MATCH (h:WarehouseLiveCheckpoint)
RETURN h.run_id AS run_id, h.seq AS commit_seq, h.tick AS tick;
```

实现使用事务驱动替身和真实live控制路径进行验证；进程PID测试使用真实子进程和真实Ray writer actor。尚未连接用户的真实Neo4j进行端到端写图，启动器会在实际执行时首先验证目标数据库。

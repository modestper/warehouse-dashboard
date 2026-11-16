# 数据集注册与启动

项目现有9套 `data/examples/` 任务集均已注册到 `config/live-datasets.json`。统一使用 `ops/start_neo4j_live.py --dataset-id <别名>`，不需要重新生成或复制 CSV。

| `--dataset-id` | 数据目录（相对 `data/examples/`） | 任务数 | 任务释放期 | 地图 / 移动模型 |
|---|---|---:|---|---|
| `medium19` | `wheeled-medium-sample19` | 19 | 30分钟 | medium 48架 / wheeled |
| `medium1728` | `wheeled-medium-peak` | 1728 | 1小时 | medium 48架 / wheeled |
| `sample19` | `warehouse-v5` | 19 | 30分钟 | 原36架 / legacy |
| `peak1728` | `regression/peak` | 1728 | 1小时 | 原36架 / legacy |
| `day` | `regression/day` | 6912 | 24小时 | 原36架 / legacy |
| `peak-2h` | `regression/peak-2h` | 768 | 2小时 | 原36架 / legacy |
| `hotspot-2h` | `regression/hotspot-2h` | 924 | 2小时 | 原36架 / legacy |
| `continuous-2d` | `regression/continuous-2d` | 1034 | 48小时 | 原36架 / legacy |
| `seven-patterns` | `seven-patterns-20261028` | 50112 | 7天 | 原36架 / legacy |

`medium19` 与 `medium1728` 分别加载 `config/wheeled-medium-100.json`，行驶速度1.5米/秒、混合型号和有限换电；`medium19` 同时加载低电扰动。五套 `regression` 输入分别加载各自原 `config.json`、`disturbances.json`，保留0.8米/秒的移动及原充电策略。`sample19` 加载原样例低电扰动；`seven-patterns` 使用legacy默认配置，不附加样例低电扰动。

注册旧数据保留其原地图和业务含义，不表示它们已变成当前medium地图的新实验输入。

## 查看与选择

离线查看全部输入：

```bash
./run_in_lade.sh python ops/start_neo4j_live.py --list-datasets
```

沿用已有 Neo4j 凭证，启动全天任务的示例：

```bash
KG_BACKEND=neo4j \
KG_RUN_ID=warehouse-day-001 \
KG_WRITE_MODE=process \
KG_STATE_SOURCE=neo4j \
KG_RECORD_AGENT_STATE=1 \
NEO4J_URI=bolt://127.0.0.1:47687 \
NEO4J_USER=neo4j \
NEO4J_DATABASE=neo4j \
./run_in_lade.sh python ops/start_neo4j_live.py \
  --dataset-id day \
  --host 127.0.0.1 --port 4173 \
  --backend ray --ray-address local --ray-num-cpus 5
```

未配置密码时启动器交互提示输入；数据库名必须使用实例中实际存在的库。页面端口应空闲，每次新实验使用新的 `KG_RUN_ID`。切换其他任务集时更换 `--dataset-id`，静态导入与运行态写入使用同一注册标识。

此启动器对9套输入均采用无本地 SQLite journal 的 Neo4j 提交方式，机器人状态推进仍使用Ray actor；旧输入采用legacy物理模型，medium输入采用wheeled物理模型。`main/ray/process` 仍只决定写图主体，不选择任务集。

当前用户的 `/home/chioe/.bashrc` 中 `s4173` 仍固定传入 `--dataset-id medium19`，只接受运行编号和写图方式两个位置参数。本次未修改该文件；选择其他任务集可先使用上面的直接启动命令。

## 注册字段

| 字段 | 作用 |
|---|---|
| `dataset` | 包含 tasks.csv、initial_inventory.csv、manifest.json 的目录 |
| `scene` | 与 manifest 的 scene_sha256 匹配的地图 |
| `mobility` | `legacy` 或 `wheeled`，按注册项选择物理模型 |
| `config_json` / `config` | 可选的机器人与调度配置 |
| `disturbances_json` | 可选的原实验扰动 |
| `locations_csv` | 与该地图配套的静态图谱位置表 |
| `graph_dataset_id` | 独立的 Neo4j Dataset 标识，避免不同输入的同名任务/货物节点混用 |

`--registry` 可指定其他注册表。保留 `config/live-wheeled-datasets.json` 的两个medium别名，配置与统一注册表保持一致。已有历史批处理的 `--dataset <路径>` 入口继续可用，但其journal行为由该旧入口决定。

本次注册与测试未向实际数据库导入数据，也未重启已有服务。新代码在后续新启动中生效；未执行完整七日或九套Ray/Neo4j实验。

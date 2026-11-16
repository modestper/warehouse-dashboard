# 固定 runId 与页面新开运行的关系

### 任务背景

用户在前端切换数据集并点击“保留记录，另开新run”，希望确认其影响，尤其是启动时指定固定 `runId` 后，页面新建运行是否还与编号有关。本次只读核对启动参数、前端请求、运行创建与保存逻辑，没有操作当前运行。

### 关键结论

- **新开 run 与 runId 直接相关：每次页面新建都会创建新的运行身份。** 启动时的 `KG_RUN_ID` 或 `--run-id` 只指定首次运行的编号，没有把该服务后续所有实验永久绑定到同一编号。
- 页面新建请求只传所选 `dataset_id` 与防止过期操作的 `expected_run_id`，没有指定新运行的编号；后端因此自动生成 `wh-live-<UTC时间>-<随机后缀>`。
- 新 run 会从所选数据集的初始时间、库存、机器人状态和任务进度开始。旧运行资源关闭，已有记录保留；新运行不是原 run 内的一次换任务，也不是继续原进度。
- 新运行保存和写图使用新的 `sim.run_id`，输出目录也使用新编号；按启动时旧编号查询，不能据此查到后来新 run 的执行过程。
- 页面不会把自动生成的新编号写回 shell 的 `KG_RUN_ID`，也不会修改原启动命令。再次用原固定编号启动新实验时，现有启动器会检查输出和 Neo4j 中是否已存在，并拒绝重复占用。
- “保留记录”与“可以恢复执行”是不同能力；当前 `--no-journal` 启动路径明确不支持 `--resume-run-id`，不能把旧记录保留理解为能直接用同一启动命令续跑。

#### 编号变化示例

假设启动时指定 `KG_RUN_ID=experiment-001`：

| 操作 | 当前运行编号 | 运行含义 |
|---|---|---|
| 首次启动 | `experiment-001` | 使用命令指定的首轮实验编号 |
| 暂停后继续 | `experiment-001` | 在首轮进度上继续 |
| 页面另开新 run | `wh-live-<时间A>-<后缀A>` | 从所选输入初始条件开始第二轮实验 |
| 页面再次另开新 run | `wh-live-<时间B>-<后缀B>` | 再开始第三轮独立实验 |

旧编号对应的实验记录会继续存在，新记录分别关联各自的新编号。相同任务输入可以复用，但运行身份不同。

上一轮只读排查实际观测到页面当前运行编号为 `wh-live-20261104T072445Z-ced223d0`，数据集为 `sample19`，输出目录为 `data/neo4j-runs/wh-live-20261104T072445Z-ced223d0`。这是先前采样的事实，本次未重新请求实时快照，不将其当作之后始终不变的编号。

#### 代码依据

- `ops/start_neo4j_live.py`：`--run-id` 默认读取环境中的 `KG_RUN_ID`，再作为启动参数传给 `ops/warehouse_live.py`。
- `ops/warehouse_live_runtime.py` 第 293 行：`self._initial = (dataset_id, run_id, resume_run_id)` 保存首轮输入；第 373 行使用这组参数创建首次运行。
- `dist/original-platform.mjs:platformControl()`：`reset` 映射成 `new_run`；请求的 `expected_run_id` 是当前 run 的校验值，不是要求新运行沿用此编号。
- `ops/warehouse_live.py:/api/control`：页面新建转换为 `value={'dataset_id': ident}`；此 HTTP 接口未开放新 run 的自定义 `run_id` 字段。
- `ops/warehouse_live_runtime.py:_control()`：读取新建请求的可选 `run_id`，页面路径没有该字段，因此实际传入 `None`。
- `ops/warehouse_live_runtime.py:_create_run_impl()`：未给编号时按时间和随机后缀生成；输出目录为 `output_root / run_id`，并用这个编号构造仿真器。
- `backend/warehouse_knowledge_writer.py:append()`、`backend/neo4j_live_store.py:append()`：使用检查点内的 `run_id` 保存对应运行记录。
- `ops/start_neo4j_live.py:verify_database()` 和 `main()`：检查 `Run`、`WarehouseLiveCheckpoint` 以及输出目录中的已有编号，阻止重复新建覆盖。
- `ops/warehouse_live.py:main()`：无 journal 路径拒绝 `resume_run_id`。

### 执行过的操作

- 使用 `rg -n`、`sed -n` 追踪上述启动、创建、请求和保存逻辑。
- 阅读前次临时快照 `/tmp/warehouse-ui-snapshot-20261104-a.json`，核对当时真实的运行编号与输出目录。
- 未调用控制 API、重启服务、创建新运行或修改数据库；未运行测试，本次为现有参数与控制流说明。

### 修改内容

- 仅新增本文档，没有修改编号生成方式、页面控件、启动参数或现有运行。

### 后续建议

- 记录实验结果或查询图谱时，以页面当前显示的 runId 为准，不能一直沿用首次启动的编号。
- 若希望每轮都使用自己指定的可读编号，当前可以为每次启动分配新的 `KG_RUN_ID`；页面自定义新 run 编号属于尚未实现的交互扩展，本次未修改。
- 本次无需用户确认。

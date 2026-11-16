# 页面自定义新 runId

### 任务背景

用户希望在前端“保留记录，另开新run”时自己填写新运行编号，避免每次都只能使用系统自动生成的编号。默认兼容现有行为：留空自动生成，填写则使用自定义编号。

### 关键结论

- 已在统一控制台的数据集操作区域新增“新 runId（可选）”输入框。
- 编号会经前端参数构建、HTTP `/api/control`、运行器进入实际新运行、输出目录和检查点，不是仅修改页面显示。
- 留空或只输入空格继续使用自动编号；输入两端空格会去掉。编号以英文字母或数字开头，允许英文字母、数字、下划线、短横线和点，长度为1～128位。
- 重名保护同时检查本地输出目录与 Neo4j 中的 `Run`、`WarehouseLiveCheckpoint`、`WHRun`；已存在则拒绝新建，保留当前运行和旧记录。图谱检查不可用时同样拒绝新建，当前运行不因这次校验错误被标记为失败。
- 创建成功后清空输入框；失败时保留填写内容，便于修改。新编号和用于防止过期操作的当前 `expected_run_id` 保持独立。
- 20项 Python 接口/运行测试、11项前端转换测试全部通过，隔离浏览器的5类交互检查通过。
- 当前4173服务没有重启或被操作。后端参数接口需要在服务下次重启后生效；立即重启会影响正在执行的 run，需另行确认。

使用方式：

```text
暂停当前运行 → 选择下一次数据集 → 填写新 runId → 点击“保留记录，另开新run”
```

例如填写 `exp-20261104-01`，新运行、输出目录、检查点和后续运行记录就使用这个编号。希望自动命名则保持输入框为空。

HTTP 请求示例：

```json
{
  "action": "new_run",
  "dataset_id": "sample19",
  "run_id": "exp-20261104-01",
  "expected_run_id": "当前页面的旧运行编号"
}
```

`run_id` 仅允许用于 `new_run`，不会作为普通暂停、继续等操作的编号修改参数。当前操作仍需先暂停；旧记录保留，新运行从数据集初始状态开始。

### 执行过的操作

- 阅读 `AGENTS.md`、当前 Git 状态、前端控件及参数适配、后端 HTTP 和运行创建逻辑。
- 用 `rg -n`、`sed -n` 和 `git diff` 追踪数据流与已有编号保护。
- 运行 `node tests/original-platform.test.mjs`：11项通过。
- 运行 `PYTHONDONTWRITEBYTECODE=1 ./run_in_lade.sh python -m unittest -v tests.test_warehouse_live_http tests.test_warehouse_live_runtime`：20项通过，最终耗时33.254秒。
- Python测试覆盖自定义/留空编号、非法输入拒绝、128位边界、HTTP到真实检查点的传递、本地重名时旧现场保留、三类图谱历史编号冲突、图谱检查失败时保留当前运行。
- 浏览器脚本 `/tmp/warehouse_custom_runid_ui_20261104.mjs` 在隔离页面模拟所有接口，验证自定义提交显示、成功清空输入、重名保留现场与输入、留空自动编号、非法编号不提交5类交互。未访问当前仿真服务，JavaScript运行异常为0；重名测试预期收到HTTP400。
- 预览截图：`/tmp/warehouse-custom-runid-20261104.png`，已检查输入框样式。
- `git diff --check` 通过。
- 沙箱内 FastAPI TestClient 初始化受限、Chromium启动被权限阻止，获执行许可后在沙箱外完成隔离验证。首次新增集成测试误将检查点封装当作原始状态读取，已改用仓库既有 `read_checkpoint()` 后通过；浏览器模拟新帧已同步清除旧tick的运动证据。
- 没有向当前4173发送控制操作，没有启动真实Ray仿真或写入真实Neo4j；当前运行未重启。

### 修改内容

| 文件 | 修改目的 |
|---|---|
| `dist/index.html`、`dist/styles.css` | 增加输入框、规则提示和样式，更新资源版本标记 |
| `dist/live-model.mjs` | 统一构造并校验数据集、新编号参数 |
| `dist/original-knowledge.mjs` | 在创建前校验输入、在确认中展示编号，成功后清空输入 |
| `dist/original-platform.mjs`、`dist/app.js` | 将新编号传入实际控制请求，同时保留当前run校验 |
| `ops/warehouse_live.py` | 为 `new_run` 接口开放可选 `run_id`，在进入运行线程前校验格式 |
| `ops/warehouse_live_runtime.py` | 自定义编号创建前检查本地与图谱重名，保留原运行 |
| `tests/original-platform.test.mjs` | 验证自定义参数、留空兼容、非法与长度边界、当前编号隔离 |
| `tests/test_warehouse_live_http.py` | 验证接口透传与非法输入不进入运行线程 |
| `tests/test_warehouse_live_runtime.py` | 验证真实检查点身份、重复编号现场保留与图谱只读检查 |

工作区原有其他修改保留，没有提交、还原或覆盖它们。

### 后续建议

- 服务重启并刷新页面后，即可使用新增输入框。
- 若在当前运行结束前立即重启，会停止当前run。按照 `AGENTS.md` 中“可能影响已有实验结果或线上服务”需先确认的要求，本次未自行重启。
- 下次启动仍需使用未占用的启动 `KG_RUN_ID`；进入页面后，各次新run可分别填写新的编号。当前无journal启动器仍不支持恢复旧run，此功能不改变恢复边界。
- 先前《固定runId与页面新开运行的关系》中“页面不能自定义编号”的描述属于本次改动前的行为，以本文和当前源码为准。

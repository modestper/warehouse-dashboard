# 网页最大 tick 参数与停止控制

### 任务背景

用户要求在网页前端增加可自行填写的最大 tick 参数，与自定义 runId 一起传入，用于限制新运行最多推进的仿真步数。按新运行从 tick 0 开始、达到上限自动暂停并保留记录、留空不限制的语义实现。本次未重启或控制现有实验。

### 关键结论

- 网页固定数据集区域新增“最大 tick（可选）”数字输入框，与新 runId 一起用于创建下一次运行；保留现有 runId 弹窗，弹窗显示本次最大 tick。
- 请求通过既有 `/api/control` 的 new_run 传递 max_ticks。前端与 HTTP/owner 三处均验证：正整数 1～2147483647；页面留空时省略参数，API 的 null 表示不限。拒绝零、负数、小数、布尔值、字符串形式的接口数字和越界值。
- 后端在每步完成后检查上限；到第 N 步即停止继续推进，等待已接受的异步机器人图谱工作完成，再保存最终检查点和部分运行结果。后端拒绝到达上限后的 start/resume/step，前端也禁用继续和单步。
- 达到上限的运行显示 paused、tickLimitReached=true、stopReason=max_ticks；不会被误标为全部任务完成，也不删除任务、旧 run 或图谱。results.json 标明停止原因和实际完成情况。
- 参数保存到 live-manifest.json、检查点 run_metadata、快照 metadata 与 results.json；支持 journal 路径恢复时保留上限。当前 Neo4j 无 journal 入口仍不支持恢复旧 run，此改动不扩展该恢复能力。
- 数字输入框可以修改后用于下一次新 run；不会直接改变当前运行的上限。创建失败保留填写内容；成功后清空 runId，最大 tick 留在框内便于重复同一规模实验。留空再另开新 run 可取消上限，不继承旧运行的上限。
- 已完成前端参数/控制测试与隔离浏览器验证；38 项后端测试全部通过，包括真实本地引擎的精确暂停、最终提交、越界控制拒绝、恢复、新运行不限和 HTTP 参数传递。验证不使用真实 Neo4j/Ray。

### 执行过的操作

- 阅读 AGENTS.md、Git 工作区状态、已有 runId 输入与请求逻辑、HTTP 参数验证、owner 生命周期与持久化、相关测试。
- 修改前端参数构造、输入框、样式和状态显示，更新 HTML 资源版本。
- 修改 HTTP 到 owner 的 max_ticks 参数链及后端步数限制。
- 新增边界与集成测试，并补齐既有发布点测试替身中的当前构造默认字段；恢复测试发现 run_metadata 必须在物理状态恢复前设置上限，已修正。
- 执行前端测试：

```bash
node tests/original-platform.test.mjs
node --test tests/original-platform.test.mjs tests/live-view.test.mjs tests/dashboard-view.test.mjs
```

第一条 13 项通过，第二条三个测试文件全部通过。

- 执行后端回归：

```bash
PYTHONDONTWRITEBYTECODE=1 ./run_in_lade.sh python -m unittest -v \
  tests.test_warehouse_live_http tests.test_warehouse_live_runtime tests.test_live_published_point
```

最终 38 项全部通过，耗时 61.397 秒。

- 使用 `/tmp/warehouse_max_ticks_ui_20261109.mjs` 在完全模拟接口的浏览器页面验证：上限和 runId 同时提交、取消不提交、重名保留现场与输入、小数不提交、到上限禁用继续/单步、仍可另开新 run、留空取消上限及 reset 入口。
- 截图 `/tmp/warehouse-max-ticks-20261109.png` 已查看，输入框、提示及达到上限的显示可见，浏览器 JavaScript 错误为 0。
- 执行 JavaScript 语法检查和 `git diff --check`，通过。
- 沙箱内 Chromium 启动被阻止、HTTP TestClient 无法完成；获执行许可后在沙箱外完成隔离验证。未安装新依赖，未重启现有服务、启动真实仿真或写真实数据库。

### 修改内容

| 文件 | 修改用途 |
|---|---|
| dist/index.html、dist/styles.css | 最大 tick 输入、正整数范围、提示和当前上限显示 |
| dist/live-model.mjs | 统一校验并构造 max_ticks；控制按钮识别达到上限 |
| dist/original-platform.mjs、dist/app.js | 与 runId 一起提交参数，暂停后禁用继续按钮 |
| dist/original-knowledge.mjs | 新建弹窗显示上限、校验输入、显示 tick/上限及停止原因 |
| ops/warehouse_live.py | HTTP 开放 new_run 的 max_ticks，其他动作不能夹带修改 |
| ops/warehouse_live_runtime.py | 每 run 上限、保存/恢复元信息、精确暂停、图谱 flush、拒绝越过上限 |
| tests/original-platform.test.mjs | 参数传递、空值、上下界、非法输入与控制禁用 |
| tests/test_warehouse_live_http.py | HTTP 类型校验及 runId+上限一同传递 |
| tests/test_warehouse_live_runtime.py | 真实运行停在 1/3 步、检查点/结果、恢复、空值重置和旧记录保留 |
| tests/test_live_published_point.py | 测试替身补齐当前 no_journal、kg_write_mode、record_agent_state 及上限默认字段 |

工作区原有的其他修改保留，没有提交或还原它们。本次没有新增 CLI 最大步数参数，功能作用于网页新建运行及其 HTTP/owner 参数。

### 后续建议

后端重新启动并刷新页面后使用：

```text
暂停当前 run → 选择数据集 → 填写最大 tick（例如 10000）
→ 点击“保留记录，另开新run” → 在弹窗输入 runId → 创建运行
```

前端输入也可先预填 runId。请求示例：

```json
{
  "action": "new_run",
  "dataset_id": "day",
  "run_id": "states-10000-001",
  "max_ticks": 10000,
  "expected_run_id": "当前旧运行编号"
}
```

上限是本次 run 的绝对最大 tick，不是现实运行秒数或任务条数。自然完成条件可以让运行在上限之前结束；如果要积累 10000 步逐步历史，应选足够长的 day 等输入，并保持每步记录历史。medium19 等短输入不能仅靠填写上限保证产生 10000 步。

到达上限后，如果需要另一个规模实验，修改数字并另开新 run。当前版本不支持直接提高同一 run 的上限后继续，避免混淆已保存实验范围。

现有后端进程需要重启才能加载新的 HTTP 与步进逻辑。按照 AGENTS.md 中“可能影响已有实验结果或线上服务”需先确认的规则，本次没有自行重启当前实验；可在当前运行结束后使用原启动方式和新的 runId 启动，再刷新页面。仅刷新浏览器不能更新旧后端进程。

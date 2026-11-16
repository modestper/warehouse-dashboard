# 仓储系统与 LADE 实现差距评估

评估日期：2026-10-26。

- 当前系统：`/data/zc/datasets/human_robot/warehouse-dashboard`，HEAD 为 `e721268`。
- 参考系统：`/data/zc/datasets/LaDe/viewer`，HEAD 为 `899c50c`。
- 依据：两边当前工作区源码、相关文档、本次单元测试及独立内存验证。HEAD 用于定位版本，评估对象不是只限于已提交文件。
- 边界：不修改业务代码，不连接真实 Ray、Neo4j 或模型服务，不启动或停止现有服务，不读取或覆盖已有实验检查点。

### 任务背景

用户希望知道当前仓储机器人系统，距离本机 LADE 项目的实际实现还有多大差距。比较重点是可迁移的系统能力，而不是把配送业务逐项复制到仓储业务。

本次区分三个目标：可展示的多机器人仿真、具备 LADE 同类能力的知识增强实验平台，以及研究指标或真实机器人部署。这三个目标的完成标准不同。

### 关键结论

- **基础仿真骨架已经具备。** 仓储系统有领域状态机、统一时钟、local/Ray 运行时、提案与提交、交通仲裁、实时快照、原子检查点和异常恢复入口，不需要推倒重建。
- **距离 LADE 的知识增强实验平台仍有明显差距。** 主要需要补齐六组能力：真实任务与可替换策略、时序图谱、知识查询与 LLM、故障互助接管、实验与查询评测、历史回放与长运行工具。其中部分已有雏形，尚未形成完整能力。
- **现有交通基础需要先加固。** 默认配置的独立内存仿真推进 2,000 步后，5 台机器人连续阻塞超过 100 步；最长连续阻塞 1,899 步。还复现了路径交叉漏检和拒绝前车后未重新约束后车的问题。
- **当前调度更接近任务生成与执行规则。** 普通任务在选中空闲机器人时才生成，没有独立订单池、释放时间和截止时间，也没有距离、负载、时效与资源的候选成本比较。页面等待数、队列 ETA 仍有演示公式。
- **最大的研究能力缺口是知识层。** 当前有 `run_id` 和近期事件，但没有 Neo4j、Step/AgentState 历史存储、历史查询、决策上下文检索或并发查询评测。100 个机器人 actor 不能证明支持 100 个智能体的时序图谱协同查询。
- **LADE 也不能视为全部验收通过的标准答案。** 本次 102 项相关测试中 99 项通过、3 项电量准入测试失败；源码中对应硬校验已关闭。其独立查询套件也没有全部自动接入调度循环。
- **建议先补可靠性和任务语义，再补知识闭环。** 若要估算排期，按一名熟悉两边代码的开发者、复用 LADE 设计和已有基础设施估计，最小可验证知识闭环约 15–25 人日，同类平台的主要能力约 30–50 人日。它们是待拆分的工程估算，不是实测交付承诺，也不包含实机接入、全部查询迁移或研究指标认证。

#### 1. 分维度比较

表中“缺失”指本次检查未发现对应实现；“已有”表示存在实现，不代表真实集群或长期运行已验收。

| 维度 | 当前仓储系统 | LADE 当前实现 | 判断 |
|---|---|---|---|
| 单步仿真与权威状态 | Coordinator 管理时间、任务、货架；RobotAgent 执行动作 | 环境管理任务和时间，DroneAgent 执行 | 骨架已具备，主要是业务差异 |
| local/Ray 执行 | 公共 Runtime Protocol；同一 RobotAgent；提案/仲裁/提交 | 本地环境、actor、状态代理、序列化同步 | 已具备，不应重复建设 |
| 规模含义 | 默认 100 台在线、最多 20 台持有任务 | drone 数量可配置，每台一个 actor | 当前不等于 100 台同时作业；两边均是中央调度为主 |
| 任务输入与生命周期 | 程序生成上架任务，有取货、放置与完成状态 | CSV 清洗、固定种子采样、区域 depot、释放时间、deadline | 当前缺独立任务输入与待办任务池 |
| 调度策略 | 按轮次/ID 选机器人，按公式选格位；有高度兼容和优先任务 | 规则成本、同区域优先的贪心拍卖、区域 LLM | 缺可替换策略及可比较的规则基线 |
| 空间交通 | 固定折线路线、端点距离/交换检查、阻塞告警 | 直线飞行，没有仓储避碰 | 仓储有专属模块，但本次验证发现可靠性缺口 |
| 能源与容量 | 百分比电量、移动耗电、返回后充电；单任务/单货物 | 距离代理电量、队列容量与载货容量分离、返航和互助 | 当前资源模型较简化；多货物是否需要应按仓储设备决定 |
| 故障互助 | 阻塞等待和 warning 分支；没有重派/交接协议 | 故障、低电、时效风险触发互助；未取货重派、已取货交接 | 核心协作能力缺失 |
| 知识存储 | 内存任务、货架、最多 100 条显示事件，JSON 检查点 | 内存/Neo4j、Run、实体、Event、Step、可选 AgentState、索引 | 最大结构性差距之一 |
| 历史查询与模型 | 未发现实现或依赖 | 区域历史上下文、规则/LLM 回退；56 个通用查询和 56 个上下文查询 | 相关链路缺失，需要仓储 schema 和查询适配 |
| 恢复与长期运行 | 定期原子保存、resume、Ray 失败恢复及暂停 | 校验和、配置校验、数据库进度核对、headless、supervisor、退避 | 基础已有，长期实验工程不足 |
| 展示与回放 | 实时 WebSocket、搜索筛选、控制、当前机器人 CSV 导出 | SSE、浏览器帧时间轴、离线 trace、手动图查询 | 实时展示已有，历史回看与查询缺失 |
| 自动化验证 | 本次 8 项测试通过，主要覆盖 local 和简单交通场景 | 领域、策略、fake Ray/KG、恢复、查询、前端等多类测试 | 当前缺故障、后端一致性和知识链路验证；不能用测试数量计算完成率 |
| 实验评测 | 固定 seed、少量参数与状态导出 | headless、查询并发压测、JSONL、avg/p95/p99、吞吐、图数据构建与备份 | 缺可复现的实验与性能报告工具链 |

当前值得保留的实现包括显式 Runtime 接口、唯一权威 Coordinator、格位预留和能力限制、确定性 PRNG、提案版本检查，以及 WebSocket 最新帧队列。迁移 LADE 的策略、存储和查询设计即可，无需替换现有 FastAPI 或把 WebSocket 改成 SSE。

#### 2. 优先处理的现有问题

**2.1 持续阻塞：默认场景也能复现。**

使用全新 `SimulationCoordinator(LocalRobotRuntime(), SimulationConfig())`，在内存中运行，不写检查点：

| tick | 仿真秒数 | 已完成 | 已生成任务 | 连续阻塞超过 100 步的机器人 | 最长连续阻塞 |
|---:|---:|---:|---:|---|---:|
| 500 | 125 | 16 | 21 | 82、87、94、95、99 | 399 步 |
| 1,000 | 250 | 17 | 23 | 82、87、94、95、99 | 899 步 |
| 2,000 | 500 | 21 | 26 | 82、87、94、95、99 | 1,899 步 |

最后仍有 5 台处于 `toRack`，另有 1 台返回中。1,899 步按每步 0.25 秒折合 474.75 秒。系统整体仍在完成其他任务，因此本次证据是**部分机器人持续阻塞**，不能直接表述为全局停摆或证明永远无法解锁。

另一个 `flow_rate=3600` 的场景保留 100 台机器人、20 台活动上限和 0.25 秒步长；2,000 步完成 82 个任务，13 台连续阻塞超过 100 步，最长同样为 1,899 步。该流量是主动提高的命令行配置，不是默认值。

本次两次本地运行耗时约 13.2 秒、15.4 秒，仅用于完成短时行为验证，不是 Ray 扩展性、服务延迟或性能达标结果。

**2.2 交通仲裁不能保证完整运动过程和提交后最小距离。**

源码：[backend/traffic.py:24](/data/zc/datasets/human_robot/warehouse-dashboard/backend/traffic.py:24)。以半径 `0.12` 直接构造合法 proposal：

- 交叉路径：A 从 `(0, 0)` 到 `(0.2, 0)`，B 从 `(0.1, -0.1)` 到 `(0.1, 0.1)`，两者均获准。若以同一 tick 内匀速线性移动解释，两者会在中点相遇；当前比较端点与交换条件，未检查这类过程冲突。
- 拒绝传播：A 从 `(0, 0)` 到 `(0.15, 0)`；B 从 `(0.2, 0)` 到 `(0.35, 0)`；C 在 `(0.35, 0)` 执行静止放置。B 因 C 被拒绝，A 仍获准，提交后 A、B 距离为 `0.05`，小于 `0.12`。

第二例直接表明，前车拒绝后需要重新核对后车与实际停留位置。第一例属于单步运动模型的验证边界，不能据此声称本次默认仿真已经出现实体碰撞。当前也没有真实机器人几何、制动距离和地图障碍约束模型。

**2.3 任务流量、等待数量和实际订单尚未统一。**

源码：[backend/simulation.py:152](/data/zc/datasets/human_robot/warehouse-dashboard/backend/simulation.py:152)、[backend/simulation.py:185](/data/zc/datasets/human_robot/warehouse-dashboard/backend/simulation.py:185)。

普通任务不是独立到达后进入队列，而是选到空闲机器人并有流量额度时才创建。`task_credit` 还是有上限的数量额度，不能替代有 ID、到达时间、优先级和 deadline 的待办任务池。优先任务可以等待，但当前主要保存为计数。

页面 [dist/app.js:1451](/data/zc/datasets/human_robot/warehouse-dashboard/dist/app.js:1451) 用 `Math.max(3, ...)` 推算等待任务数；[dist/app.js:1494](/data/zc/datasets/human_robot/warehouse-dashboard/dist/app.js:1494) 用 ID 和列表序号生成 ETA。接入后端后这些公式仍会运行，不能直接作为调度实验指标。

**2.4 长期任务供给会遇到有限格位。**

后端创建固定货架，初始占用由 seed 决定；默认种子共有 252 个空格位。完成上架会把格位标为占用，没有看到出库、格位释放或明确的终止结果。因此即使消除交通问题，当前配置也不能无限持续入库；机器人高度能力还会进一步约束可用格位。

前端离线演示的 `prepareTask()` 有回收格位逻辑，后端 `_assign_task()` 找不到格位时直接返回失败。两种模式的长期语义不同。正确补齐方式应是明确有限批次实验或实现出入库任务，不能为了保持动画而静默删除真实库存。

**2.5 恢复已有实现，但尚缺关键验证。**

源码：[backend/server.py:142](/data/zc/datasets/human_robot/warehouse-dashboard/backend/server.py:142)、[backend/runtime.py:122](/data/zc/datasets/human_robot/warehouse-dashboard/backend/runtime.py:122)、[backend/checkpoint.py:10](/data/zc/datasets/human_robot/warehouse-dashboard/backend/checkpoint.py:10)。

- `ray.get()` 没有超时；挂起的 actor 不一定抛错进入恢复逻辑。
- 同步 Ray 调用和检查点 I/O 在异步服务循环中执行，存在占用事件循环的风险，需要测量 API/流式输出延迟并明确线程或执行器边界。
- 当前检查点有原子替换和文件 fsync，但缺少内容校验和及目录 fsync；配置恢复主要检查 schema 和机器人数量。
- 现有仓储测试未覆盖真实或 fake Ray 的部分提交、actor 故障、恢复失败及 local/Ray 行为一致性。

这些是源码可见的工程缺口；本次没有通过真实 Ray 故障注入验证，不把风险写成已经发生的线上故障。

#### 3. LADE 可以复用什么、不能照搬什么

| 可参考的 LADE 文件 | 仓储系统中的建议对应物 |
|---|---|
| [simulation/planner.py](/data/zc/datasets/LaDe/viewer/simulation/planner.py:11) | 独立的规则成本、批量决策、模型适配；统一提交层校验策略输出 |
| [simulation/knowledge.py](/data/zc/datasets/LaDe/viewer/simulation/knowledge.py:235) | memory/Neo4j 存储接口；Run、Robot、Task、Rack、Slot、Event、Step、RobotState |
| [simulation/delivery_sim.py](/data/zc/datasets/LaDe/viewer/simulation/delivery_sim.py:478) 的互助函数 | 阻塞超时/故障/低电触发重派，货物与格位预留的显式转移协议 |
| [simulation/checkpoint.py](/data/zc/datasets/LaDe/viewer/simulation/checkpoint.py:114) 和 headless/supervisor | 内容校验、运行配置核对、无页面执行、异常重启与后续数据库进度校验 |
| [scripts/lade_llm_context_queries.py](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:564) | 按机器人、区域、run、step 构建上下文，分别记录查询和完整上下文耗时 |
| [scripts/lade_cypher_benchmark.py](/data/zc/datasets/LaDe/viewer/scripts/lade_cypher_benchmark.py:636) | 可控并发、查询日志、成功率、吞吐和延迟分位数 |
| [src/dynamic_timeline.mjs](/data/zc/datasets/LaDe/viewer/src/dynamic_timeline.mjs:15) 与离线回放 | 缓存帧、回看游标、恢复实时、离线 trace |

优先迁移契约与框架，改写仓储字段和约束：城市/depot、无人机航程、配送截止时间映射不能原样复制。仓储的一次一件或单任务模式可能是合理的设备约束，不应为了表面对齐而强行增加多货物。

LADE 当前还存在以下边界：

- `DroneAgent.can_accept_task()` 和 `DroneStateProxy.can_accept_task()` 都只调用 `can_accept()`；此前的电量硬校验被注释。电量仍参与成本、执行、返航和互助筛选。本次相关三项测试失败与这一差异一致，不擅自判断该实验选择应恢复还是保留。
- 默认规则派单主要读取内存状态；区域 LLM 模式才通过知识存储读取历史表现和事件。Neo4j 写入存在不等于知识查询参与每次决策。
- 通用 `q0–q55` 和上下文 `i0–i55` 各 56 个查询已在目录中，但属于独立调用工具；当前调度没有自动执行整套上下文查询。
- 互助是中央协调和固定交接点模型，不代表双方在真实空间完成同步会合与实物确认。
- headless 检查点完整接口主要位于 Ray 环境；数据库与检查点仍是两个系统，没有跨存储原子事务保证。
- LADE 的早期 HANDOFF 与部分说明存在滞后；本次以当前源码和实际测试为准。

#### 4. 距离研究指标还有多远

LADE 的 `项目目标.txt`、`task2指标.txt` 涉及百万规模图数据、至少 100 个智能体节点协同查询、端侧查询延迟、指定环境的多轮测试和第三方报告。当前仓储系统缺少承载这些测试的知识 schema、数据构建、查询接口、并发客户端和报告链路。

建议区分五个数字：机器人总数、同时执行任务数、数据库历史状态规模、并发查询客户端数、实际设备节点数。`robot_count=100` 只直接说明第一个；默认活动上限 20 主要约束第二个。两者都不能证明百万图数据或百客户端查询性能。

例如，100 台机器人每步各写一条历史状态，10,000 步便有 100 万条状态记录；每步 0.25 秒时对应 2,500 秒仿真时间。这里仅是数量推算，未包含其他节点/关系，不是容量或写入性能实测。应先确定采样、保留与批量写入策略，再扩规模。

本次没有验证任一系统达到 200ms/500ms 等研究指标，也没有把 LADE 已有工具等同于这些指标已通过认证。向量、多模态或端侧部署应作为单独目标评估。

### 执行过的操作

**主要阅读范围**

- 仓储：`AGENTS.md`、`README.md`、`requirements.txt`，全部 `backend/*.py` 和 `tests/*.py`，前端 `dist/app.js` 的任务、指标、后端快照、断线、时间轴和导出代码。
- LADE：`AGENTS.md`、`README.md`、`HANDOFF.md`、研究目标与指标；今天的架构、状态管理、知识查询调用链记录；领域、策略、Ray、知识、检查点、图查询的关键实现，以及 headless、supervisor、查询评测、时间轴和离线回放相关代码。
- 通过 `rg --files`、`rg -n`、`sed -n`、`cat`、`git status --short`、`git rev-parse --short HEAD` 定位代码并记录边界。

**本次实际验证**

当前仓储目录没有 `./run_in_lade.sh`，因此使用参考项目包装器的绝对路径，并加 `-B` 避免产生 Python 字节码：

```bash
# 工作目录：warehouse-dashboard
/data/zc/datasets/LaDe/viewer/run_in_lade.sh python -B -m unittest discover -s tests -v
node --check dist/app.js

# 工作目录：/data/zc/datasets/LaDe/viewer
./run_in_lade.sh python -B -m unittest \
  tests.delivery_sim_test tests.planner_test tests.knowledge_test \
  tests.ray_delivery_sim_test tests.checkpoint_test \
  tests.run_headless_sim_test tests.lade_llm_context_queries_test
```

| 验证 | 本次结果 |
|---|---|
| 仓储单元测试 | 8/8 通过，约 1.19 秒 |
| 仓储 JavaScript 语法 | 通过；不代表浏览器验收 |
| LADE 选定相关单元测试 | 102 项，99 通过、3 失败；不是全仓测试 |
| 查询目录只读导入统计 | 通用 56 个，上下文 56 个 |
| 仓储默认配置内存运行 | 2,000 步，有进度但 5 台持续阻塞 |
| 仓储高流量内存运行 | 2,000 步，有进度但 13 台连续阻塞超过 100 步 |
| 交通边界 proposal | 复现交叉路径获准及拒绝前车后后车距离不足 |

LADE 失败项：

1. `test_auction_retries_other_region_when_same_region_drone_lacks_battery`
2. `test_battery_insufficient_for_task_and_return_prevents_assignment`
3. `test_ray_reassigns_task_when_preferred_actor_rejects_assignment`

这些 Ray/KG/LLM 单元验证使用 fake/mock，测试输出中的索引创建、OOM 与恢复日志是模拟结果，不表示操作了真实数据库或集群。

内存运行的核心可复现步骤如下；执行时仍通过上述环境包装器使用 `python -B -`：

```python
from backend.runtime import LocalRobotRuntime
from backend.simulation import SimulationConfig, SimulationCoordinator

# 默认：robot_count=100, max_active_robots=20,
# step_seconds=0.25, flow_rate=48, seed=20261013
config = SimulationConfig()
# 高流量对照仅将上一行替换为 SimulationConfig(flow_rate=3600)
sim = SimulationCoordinator(LocalRobotRuntime(), config)
try:
    for _ in range(2000):
        sim.step_once()
    states = [payload['state'] for payload in sim.robot_snapshots]
    print(sim.tick, sim.elapsed, sim.completed, len(sim.tasks))
    print([(s['robot_id'], s['blocked_steps'])
           for s in states if s['blocked_steps'] > 100])
finally:
    sim.close()
```

没有运行真实集群测试、数据库集成、LLM 请求、浏览器交互验收、长时间稳定性测试或查询性能压测。

### 修改内容

仅新增本文档 `docs/codex_notes/10/26-仓储系统与LADE实现差距评估.md`。没有修改两边业务代码、配置、依赖和实验数据；保留已有未跟踪文档。LADE 只作为只读参考。

### 后续建议

按以下里程碑推进；先补齐可验证的最小链路，再扩展查询数量和展示功能。

| 顺序 | 工作 | 完成判据 | 粗略投入 |
|---|---|---|---|
| P0 | 交通提交后约束、过程冲突检查、阻塞检测与解锁/重规划；关键回归验证 | 两个反例被正确处理，默认 100 台场景不再无限静默等待，有明确失败或恢复结果 | 4–7 人日 |
| P1 | 独立任务池、任务源/固定种子负载、真实等待数和 ETA、可替换规则策略、格位耗尽语义 | 同一任务集可复现实验，指标从后端事实计算，策略输出统一校验提交 | 4–7 人日 |
| P2 | memory/Neo4j 接口、run 隔离、事件与 Step/RobotState、索引、3–5 类核心查询 | 能追溯某台机器人某一步状态与任务归属，重复写入不产生语义重复 | 5–8 人日 |
| P3 | 少量上下文查询接入决策、可选 LLM、超时/异常规则回退、决策日志 | 记录检索→决策→执行，能比较规则、规则+知识、LLM+知识三种配置 | 3–6 人日 |
| P4 | 故障/低电/阻塞任务重派与货物交接，格位预留和所有权转移 | local 与 fake Ray 场景一致，失败重试不丢货、不重复持有任务 | 5–8 人日 |
| P5 | headless、恢复加固、回放、并发查询压测、原始日志与报告 | 可重复运行、多轮比较、恢复后进度一致；分别报告查询/上下文/决策耗时 | 7–12 人日 |

合计约 28–48 人日，规划时可取 **30–50 人日，约 6–10 个全职工作周**。前提是一名熟悉代码的开发者、沿用当前二维仿真、已有可用基础设施，并限定查询与场景范围。完整任务拆分后应重新估算；全部 112 个查询逐条迁移、生产运维、真实集群扩展验收、实机运动控制和第三方指标测试均未计入。

若只做最小知识闭环，可先完成 P0/P1/P2 中必要部分，接一组小查询并保留规则调度，约 15–25 人日。若目的只是可信的仓储展示，则优先 P0/P1 和真实指标，暂时无需引入 Neo4j 或 LLM。

本次评估已完成，没有需要用户确认的操作。后续实施属于新的代码修改任务，可按用户选择的目标裁剪上述里程碑。

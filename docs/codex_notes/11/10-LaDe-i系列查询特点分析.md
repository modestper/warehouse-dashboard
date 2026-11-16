# LaDe i 系列查询特点分析

### 任务背景

用户希望先分析 LaDe 原项目 i 系列查询的特点，为后续仓储机器人操作查询设计提供依据。本次依据 `/data/zc/datasets/LaDe/viewer/scripts/` 当前源码核对56条内置查询、查询包、时间语义和并发调用方式。没有修改 LaDe 源码，没有执行 Cypher、压测或数据库操作。

### 关键结论

- i系列是面向单个机器人的决策上下文目录：重点回答“我的状态、任务、风险、可考虑的动作、附近资源和近期反馈”，并非由查询直接完成调度或操作。
- 当前共有56条 `i0～i55`，基础12条在 `lade_llm_context_queries.py`，扩展44条在 `lade_llm_context_templates.py`；每条都有编号、语义名称、类别、参数、说明、Cypher和时间范围标记。
- 查询按任务决策、能源安全、请求/提供帮助、进展、恢复、记忆、区域态势和数据就绪组织，可按包、编号范围或类别选取并去重，不要求每次查询全部56条。
- 不仅返回原始事实，还在Cypher中形成候选取送动作、截止时间余量、直达时间/航程需求、同伴空余容量、历史反馈和数据就绪标记。
- 时间语义被显式区分，但并未保证整个context是一个历史一致快照：18条标记current、14条snapshot、12条event_window，其余12条属于不同混合时间范围。
- 模板刻意避免声称没有记录的能力：同伴候选不是协助承诺，直达估算不是完整路线可行性，图谱已知任务不是全部等待任务，数据就绪不是动作授权。
- 并发发生在机器人context之间，同一context内逐条执行查询；代码能记录全部结果、逐条耗时和整包耗时，但准备上下文并不等于已经调用LLM或接管机器人行为。
- 对照仓储当前实现，最值得学习的是操作组包、派生指标、执行反馈和数据质量；本项目已有五包框架，但机器人客户端目前主要返回测量指标，若要用于决策还需提供真实事实、候选及解释，并补足库存、道路和换电资源等数据。

#### 1. 以机器人为中心，而不是以全库报表为中心

大多数查询通过 `run_id` 与 `drone_id` 缩小范围，再向外扩展到自身任务、同区域、附近同伴或相关历史。少量全局查询，例如i9，不需要drone_id，但作为整包的态势补充。

例如：

- i0：自己的当前状态；i1：不晚于指定Step的最近状态。
- i19/i20：自己的待取任务与已载货投递目标。
- i32：相对于自己的附近可考虑同伴。
- i38：从自己当前位置出发的已知返航余量。

这里读取的是配送图中的 `Drone/Task/AgentState/Event/Region/Step`，不能只替换名字就假定适用于仓储的 `Robot/WarehouseTask/RuntimeEvent` 模型。

#### 2. 按操作目的组包，模板可以复用

| 包名 | 希望支持的判断 | 代表信息 |
|---|---|---|
| `task_decision` | 下一步考虑取什么、送什么 | 自身能力、待取点、已载货目标、期限、候选动作、直达风险 |
| `energy_safety` | 是否有必要返航/补能 | 电量配置、已知depot、单任务能源余量、充电记录 |
| `request_help` | 自己是否承压、谁可能帮忙 | 任务紧急度、已观察等待、同伴距离与容量、单任务估算 |
| `provide_help` | 自己能否作为潜在帮手 | 自身容量/运行状态、已载货任务、附近已知任务与低电同伴 |
| `task_progress` | 任务进展到哪一步 | 派单、取件、配送时间、事件和执行反馈 |
| `recovery` | 是否应排查或重新判断 | 状态新鲜度、近似停滞、运行模式、逾期、电量和数据缺口 |
| `memory_review` | 最近做过什么、效果如何 | 轨迹变化、事件、服务区域、耗时与迟到反馈 |
| `local_awareness` | 周围是否忙碌、有哪些资源 | 同伴、区域任务压力、空余队列、局部完成质量 |
| `data_readiness` | 当前依据是否足够 | 实体是否存在、状态是否过旧、缺多少采样、配置是否已知 |

另保留 `core/decision/coordination/diagnostic/all` 基础选择。一个查询可被多个包复用，例如i1出现在多个包里。`select_queries()`会对选中的查询ID去重，但不会将不同查询内部重复读取的共同事实自动合并为一次数据库调用。

#### 3. 把事实加工成接近决策的问题

**i23：下一步动作候选。** 已取货并确认由自己携带时，候选为deliver；否则为pickup。返回目标坐标、直线距离、剩余期限。排序不等于恢复了真实队列顺序，也不意味着命令已被执行。

**i24：能否明显看出期限风险。** 按“当前位置→取件点→投递点”的直达距离和速度估算最早完成时间，返回是否在该简化模型下已来不及。它没有计入完整队列绕行、等待和实际路线约束。

**i40：单任务能源余量。** 计算直达取送距离、到最近已知depot的返航距离和安全余量，与剩余可用航程比较。LaDe这里以米表示电量/航程能力，不是仓储模型中的SOC百分比，也不是已考虑真实拥堵的能耗积分。

**i33：谁可能承接某项任务。** 按其他无人机的当前位置、队列容量、速度、剩余航程筛选单任务直达候选，返回预计完成时间及余量。候选可以包括queued或delivering状态、但仍有队列余量的无人机；不能直接移植成仓储接力的“空闲空载帮手”条件。

可借鉴的是“把一个操作需要的证据和估算集中返回”；估算仍依赖业务假设，不能变成执行许可。

#### 4. 时间条件比较明确，但整包不是同一时刻快照

| 时间类型 | 读取含义 | 例子及限制 |
|---|---|---|
| current | Drone/Task等可变当前属性 | i0、i18、i23、i38；指定过去step不会自动将这些属性恢复为过去值 |
| snapshot | 指定Step之前的AgentState或Step | i1、i2、i6；最近一条可能比请求时间更旧 |
| event_window | 事件时间条件 | i4、i26、i50；有的读取最近窗口，有的如i43只设截至时间，具体看WHERE |
| 混合 | 快照位置或区域结合当前实体/配置/任务/事件 | i8、i32、i52、i55；需要保留各部分时间口径 |

同伴快照类查询常先找到自身最近的 `AgentState.step_index <= requested_step`，然后使用这个**实际解析出的Step**读取其他机器人，避免随意把每个机器人的不同时间采样混在同一同伴快照里；该Step没有采样的同伴不会凭空出现。

`temporal_scope`是说明和日志元数据，不是自动完成历史回放的数据库机制。`execute_context()`使用一个Session逐条`session.run()`，没有把整个context包进一个固定历史事务；期间其他写者可能继续推进。

`time_minute`通常可以由指定Step解析得到，也允许显式参数。`build_params()`优先保留用户`--param`覆盖值，因此复现实验需核对日志中的实际参数，不能仅看外层context标签。

#### 5. 历史记忆与动作反馈有实际用途

- i14：首末采样变化，压缩位置、队列、载货与电量变化。
- i15：各状态出现了多少采样，不将样本数量直接当连续持续时间。
- i16：用窗口位置范围形成“近似静止”证据，不直接宣称机器人故障。
- i25～i31：把当前任务或本人近期任务与真实派单/取件/完成事件联系起来。
- i50：派单到取件的反馈，识别picked_up、reassigned、awaiting_pickup，避免将尚未取件直接标记为失败。
- i51：释放到完成耗时和逾期分钟；i54：已观察到的取件到完成耗时。

这类查询支持“观察执行结果，再调整判断”，不只是查询下一步动作。返回事实仍有采样、窗口、结果上限和事件覆盖限制；复杂事件关联不等于完整恢复所有历史货物责任。

#### 6. 专门查询数据是否值得相信

i13返回状态年龄与新鲜度；i52并列比较当前属性和历史快照，明确两者不同可能是合法的；i53统计窗口内缺了多少Step采样；i55检查实体、快照、容量、速度、电量信息是否齐备。

i55的返回值还显式注明 `readiness_is_not_action_authorization`：信息具备，不代表资源已预约或动作可执行。

但这不是一个统一的自动保护器：只有选中这些查询并让调用方处理结果，才能发挥作用；其他查询中的空结果、null或聚合0仍须按其语义解释，不能统一理解为“环境中没有对象”。

#### 7. 对实际图结构与能力边界比较克制

`lade_llm_context_templates.py`声明了观测图的节点、关系、字段与事件白名单，内置查询加载时进行静态检查。该检查是模式约束守卫，不是完整Cypher解析器，也不能替代实际数据库和业务验证。

源码明确不假定已记录协助事件、协助关系、完整队列顺序或历史任务归属。`request_help/provide_help`是决策输入包，不会发出求助或确认接应。

几个特别容易误解的例子：

- i7虽然名为nearby，但实际是同区域按队列/电量排序；真正的半径距离筛选看i32。
- i48查询附近**已知**未完成紧急任务，仍返回承担者，不能把结果当成可自由领取的公共任务池。
- i49按同一步电量与基准阈值找低电同伴，不等于已经满足完整动态求助判据。
- i38/i39/i40只考虑图谱中已知depot，不保证所有补能点已入图，也没有充电位/换电电池预约信息。
- i24/i33/i40的直线距离和单任务估算不提供道路冲突、完整多任务队列、抓取、库存或交接原子性保证。

#### 8. 并发与返回结果的工程特点

- 每台无人机的一次context任务负责执行其选中查询；该context内部串行，不同context通过线程池并发。
- 例如100台无人机、每台选择9条任务决策查询、repeat=1，对应900条目录查询；不是100种不同语句，也不是一次自动合并成一条查询。
- 每条返回记录都会被完整拉取并转成字典，context以语义名称保存结果；日志包含ID、类别、时间语义、参数、成功/失败、行数与耗时，还可附实际Cypher。
- 单条失败会记录错误并继续后续查询；只看空结果而忽略query_metrics，可能把“查询失败”误当“没有事实”。
- 基础runner区分单条查询时延与整包context时延；后者从worker开始执行计时，不含在线程池排队的时间。另有stages入口细分排队、session、取结果和服务端时间。
- 多条查询使用LIMIT压缩返回内容，但LIMIT不保证查询内部只访问少量节点；复杂关联和聚合仍需单独测量。

#### 9. 对仓储查询设计的启发

值得保留：机器人中心、按动作组包、参数化、显式时间范围、派生风险和候选、执行反馈、数据就绪及缺项解释。

需要按仓储重做：货架层数与臂长、地面道路和时空预约、货位库存/预约、SOC耗能、有限换电资源、单货物交接和唯一负责人；这些不是LaDe i系列已经解决的部分。

不能直接继承：当前属性与历史状态混用的风险、把直线距离当真实路径、把候选当已认领、把请求帮助包当有效帮助承诺。

#### 10. 具体怎样借鉴到当前仓储系统

| LaDe中可借鉴的设计 | 对应例子 | 仓储系统应怎样使用 | 需要改进或补充的地方 |
|---|---|---|---|
| 每条查询回答一个业务问题 | i19/i20分别读取待取和已载货目标 | 分开查询“去哪里取货”“去哪里放货”“是否满足目标货层要求” | 不把完整Robot属性返回当作已经支持全部操作 |
| 按待执行操作组织上下文包 | task_decision、energy_safety、request_help | 组织接单、取货、放货、通行、补能、接力、恢复包 | 操作触发时读取对应包，避免100台每次执行全部目录 |
| 查询结果贴近动作候选 | i23返回pickup/deliver和目标点 | 返回候选任务/货位/接收者，以及为什么入选或被排除 | 候选和已认领、已预约、最终动作分别表达 |
| 显式计算风险与余量 | i21期限余量、i24直达ETA、i40航程余量 | 返回货层能力判断、实际路网时间参考、SOC余量和截止时间风险 | 能力、SIPP和耗能计算复用现有代码，不能照搬直线航程模型或重复建立一套规则 |
| 行动前先检查数据质量 | i13/i53/i55 | 检查完整Step、观测tick、状态年龄、资源数据覆盖和缺失 | 不能只做一次加载时字段检查；每次上下文都要标记是否有足够证据 |
| 用事件验证已完成动作与效果 | i25/i50/i51/i54 | 读取取货、入库放置、出库完成、接力、换电事件，计算等待及执行耗时 | 结合货物身份、执行阶段、唯一持有者，避免只靠事件数量判断完成 |
| 所有模板有元数据和参数 | ContextQuery与build_params | 为每条查询定义操作、参数、真实来源、返回字段、时间口径及解释 | 绑定当前run的Dataset和配置；仓库型号、任务ID、阈值不能混用其他运行的数据 |
| 分别统计查询与整包耗时 | query_avg与context_avg | 同时验证业务事实正确性、整包延迟、失败率和返回量 | 100并发测试还需披露排队、持续写入与图谱规模，不能只展示单条平均耗时 |

**先解决当前五包“能测量”与“能提供决策输入”的差别。** `backend/query_c/catalog.py`已经组装了事实，但 `RobotContextClient.query()`返回给actor的主要是耗时、包行数和missing，结果按观测用途处理。若下一阶段需要机器人据此判断，应在保留测量指标之外，正式定义并返回业务事实、候选、拒绝理由、数据质量和仍待验证项目；不能仅仅增加查询条数或包名。

建议返回结构按以下职责分开：

- `facts`：原始可信事实，例如机器人型号、任务目标货层、当前持货者、状态时间。
- `derived`：使用本项目既有规则计算的能力匹配、期限余量、路径/耗能参考，并注明算法及估算范围。
- `candidates/rejections`：候选任务或接收者，以及逐项排除理由。
- `quality`：各部分观测时间、陈旧度、缺失字段、查询失败、截断和覆盖范围。
- `requires_validation`：仍需完成的任务认领、货位预约、道路冲突、能源准入或交接确认。

例如“是否能让A1处理第3层入库任务”，上下文不仅返回任务和机器人属性，还应能明确说明：A1能力限制为第1层、目标为第3层，因此能力条件不满足。这个判断复用 `task_capability_reason()`，不把型号规则再硬编码进另一份Cypher。

“X3从第3层取下出库货物后请求A1接力”，应读取剩余目标而不是重新要求A1具备已经完成的高层取货能力；同时读取帮手是否空闲空载、故障、电量和接应承诺。LaDe的同伴候选可包含尚有队列余量的delivering无人机，不能直接当作仓储帮手的准入条件。

“目标货位是否能放货”，不能用静态 `Goods-[:INITIAL_AT]->Location` 判断当前占用；需要该run下、对应时刻的真实库存与预约。当前图谱缺少可靠动态事实时，应标明缺项并限制结论，而不是将没查到当作空位。

建议优先做小规模、可核验的操作链查询：自身与就绪、已发布任务候选、源/目标能力、货物和货位、任务责任、能源、接力候选、动作反馈。不要求第一批凑齐56条。

无论由中央还是机器人自己决策，图谱读取都不代替任务认领和资源预约。自主方案还需条件更新、互斥/事务、版本及失效处理，这是i系列上下文目录之外的工作。

#### 11. 56条查询目录（源码元数据）

下表由标准库AST只读提取，不是运行查询得到的结果。完整语句仍以链接源码为准。

| ID | 名称 | 类别 | 时间范围 | 用途 |
|---|---|---|---|---|
| [i0](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:45) | current_drone_state | self | current | 读取 drone 当前属性，作为 LLM 决策的基础自我状态。 |
| [i1](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:66) | latest_agent_state | self | snapshot | 读取不晚于当前 step 的最新 AgentState，避免只依赖 Drone 快照。 |
| [i2](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:90) | recent_self_trajectory | history | snapshot | 读取最近若干 step 的自身轨迹和状态变化，给 LLM 提供短期记忆。 |
| [i3](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:111) | active_tasks_for_drone | task | current | 读取当前 drone 已分配但未完成的任务，辅助 LLM 判断下一步行动。 |
| [i4](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:142) | recent_drone_events | event | event_window | 读取当前 drone 的近期事件，告诉 LLM 刚刚发生过什么。 |
| [i5](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:166) | recent_self_performance | event | event_window | 把当前 drone 近期分配、取件、完成、超时次数压缩成 LLM 易用摘要。 |
| [i6](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:182) | current_region_load | region | snapshot | 读取当前 drone 所在区域在当前 step 的 agent 状态分布。 |
| [i7](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:205) | nearby_drones_same_region | coordination | snapshot | 读取同一区域其他 drone 的状态并按队列/电量排序；距离候选请用 i32。 |
| [i8](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:230) | known_open_tasks_in_region | task | current_with_snapshot | 读取当前区域内知识图谱已经知道的未完成且临近截止任务。 |
| [i9](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:261) | latest_global_step_summary | global | snapshot | 读取不晚于当前 step 的全局 Step 摘要，给 LLM 提供系统态势。 |
| [i10](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:284) | region_recent_events | region | snapshot_region_events | 读取当前区域近期事件，帮助 LLM 了解局部拥堵、完成和异常情况。 |
| [i11](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries.py:308) | global_region_bottlenecks | global | snapshot | 按区域聚合当前 step 的全局瓶颈，适合给 LLM 一个简短全局负载视角。 |
| [i12](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:135) | self_capability_limits | capability | current | 读取自身队列/载货容量、速度和电量配置，给 LLM 提供行动约束。 |
| [i13](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:148) | self_snapshot_freshness | readiness | snapshot | 返回自身实际快照 step 和年龄，避免 LLM 把旧状态当成当前状态。 |
| [i14](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:157) | self_recent_state_changes | memory | snapshot | 对比近期窗口首末状态，压缩自身位置、队列、载货和电量变化。 |
| [i15](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:173) | self_recent_status_samples | memory | snapshot | 按状态汇总自身历史样本，帮助 LLM 判断持续空闲或持续忙碌；不是连续时长。 |
| [i16](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:183) | self_stationary_evidence | memory | snapshot | 返回自身窗口内坐标包围范围和任务负载，辅助判断是否需要排查停滞。 |
| [i17](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:196) | self_operational_status | recovery | current | 读取自身现有运行状态和负载，识别返航/充电；不推断未记录的故障或互助模式。 |
| [i18](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:205) | self_active_task_summary | task_detail | current | 把自身当前有效任务汇总为待取件、载货和最早截止信息。 |
| [i19](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:218) | self_pending_pickup_locations | task_detail | current | 查询自身待取任务在现有 Task 上记录的取件点。 |
| [i20](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:227) | self_onboard_delivery_targets | task_detail | current | 读取自身实际携带的未完成任务和投递目标，避免把历史 CARRYING 当作当前载货。 |
| [i21](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:235) | self_urgent_task_slack | task_risk | current | 查询自身即将到期及已逾期任务的剩余时间，辅助 LLM 确定优先级。 |
| [i22](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:245) | self_overdue_active_tasks | task_risk | current | 定位自身当前仍需处理的逾期任务，用于补救和求助判断。 |
| [i23](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:255) | self_task_action_candidates | task_detail | current | 给出自身任务对应的取件/投递候选动作及直线距离，不假定图谱记录了完整队列顺序。 |
| [i24](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:271) | self_direct_completion_estimates | task_risk | current | 估算自身每个任务的直达完成时间下界；不包含队列绕行和等待，供 LLM 识别明显风险。 |
| [i25](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:285) | self_active_task_event_context | task_detail | current_task_events | 读取自身当前任务的近期分配、取件和完成事件及真实执行者。 |
| [i26](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:295) | self_recent_assignments | task_history | event_window | 读取自身近期真实派单事件及当时队列长度。 |
| [i27](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:304) | self_recent_pickups | task_history | event_window | 读取自身近期真实取件事件及当时载货数量。 |
| [i28](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:313) | self_pending_pickup_ages | task_risk | current_task_events | 为自身当前待取任务查找此前最近派单时间，识别等待取件较久的任务。 |
| [i29](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:329) | self_onboard_delivery_ages | task_risk | current | 读取自身当前载货任务的已观察配送时间和剩余截止时间。 |
| [i30](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:340) | self_recent_completed_tasks | task_history | event_window | 以真实完成事件列出自身近期送达任务与 deadline，不以 Task 最新完成状态替代历史。 |
| [i31](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:350) | self_recent_event_type_summary | task_history | event_window | 按实际事件类型汇总自身近期活动，不假定存在协助事件。 |
| [i32](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:357) | self_nearby_available_peers | peer | snapshot_with_config | 按距离、已知队列容量和电量筛选附近无人机；仅是候选信息，不声称已派遣协助。 |
| [i33](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:367) | self_task_peer_route_estimates | peer | current | 用已有任务和无人机位置/电量估算其他无人机的单任务直达路线；不是实际协助关系或完整队列可行性。 |
| [i34](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:396) | self_cross_region_peer_candidates | peer | snapshot_with_config | 按同一步位置、队列和电量返回半径内其他区域的无人机候选信息。 |
| [i35](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:406) | self_nearby_returning_peers | peer | snapshot | 读取同一步附近正在返航/充电的无人机，避免把它们当成可立即接单资源。 |
| [i36](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:417) | self_region_queue_comparison | peer | snapshot | 比较自身与同一步同区域其他无人机的队列负载，不假定存在协助承诺。 |
| [i37](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:428) | self_recent_served_regions | task_history | event_window | 按真实完成事件汇总自身近期服务过的任务区域，而不是把区域边当实时位置。 |
| [i38](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:437) | self_known_depot_return_margin | energy | current | 结合已知 depot、当前位置和安全余量估算返航阈值；depot 不完整时仅是已知范围内结果。 |
| [i39](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:455) | self_reachable_known_depots | energy | current | 列出按自身电量和安全余量可到达的已知 depot，支持返航目的地选择。 |
| [i40](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:466) | self_individual_task_energy_estimates | energy | current | 估算单项任务直达取送及最近已知 depot 的最低航程需求，不包含其他任务队列绕行。 |
| [i41](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:484) | self_recent_charge_cycle_events | energy | event_window | 读取自己的低电量、返航和充电完成事件，解释电量变化原因。 |
| [i42](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:494) | self_observed_battery_changes | energy | snapshot | 聚合自身相邻快照的电量升降，提供观察到的放电和恢复证据，不推断缺失步。 |
| [i43](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:508) | self_last_known_charge | energy | event_window | 读取截至决策时间的最近充电完成记录，判断距上次充电多久；无记录表示未知。 |
| [i44](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:518) | self_region_known_task_pressure | local_context | current | 统计自己当前区域内图谱已知的未完成、逾期与待取件任务，不声称覆盖尚未入图任务。 |
| [i45](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:530) | self_region_available_queue_capacity | local_context | snapshot_with_config | 汇总自身区域其他无人机的已知可用队列容量，提供区域资源信息。 |
| [i46](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:545) | self_region_recent_completion_quality | local_context | current_region_events | 汇总自身当前区域近期完成与迟到情况，给 LLM 一个局部服务质量视角。 |
| [i47](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:555) | self_region_recent_task_flow | local_context | current_region_events | 统计自己区域近期真实派单、取件和送达事件及参与无人机数量。 |
| [i48](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:567) | self_nearby_known_open_tasks | local_context | current | 读取自身附近图谱已知的未完成紧急任务及承担者，不假定等待任务已入图或可以重复派单。 |
| [i49](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:581) | self_nearby_low_battery_peers | recovery | snapshot_with_config | 按同一步电量和已知低电基准阈值识别附近低电无人机，不替代动态返航判据。 |
| [i50](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:595) | self_assignment_to_pickup_feedback | feedback | event_window | 为近期派单匹配下次同任务派单前的首次取件，观察实际等待时间；未取件不等于失败。 |
| [i51](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:622) | self_release_to_completion_feedback | feedback | event_window | 根据真实完成事件与 Task 释放/deadline 时间回顾端到端耗时和逾期分钟。 |
| [i52](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:635) | self_current_snapshot_comparison | readiness | current_with_snapshot | 并列返回自身当前属性与目标 step 快照的差异，提示 LLM 时间混用，不直接判为数据错误。 |
| [i53](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:648) | self_recent_snapshot_coverage | readiness | snapshot | 检查自己的短期记忆窗口中有多少不同 step 样本，避免把缺样本当成没有行动。 |
| [i54](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:658) | self_recent_delivery_feedback | feedback | event_window | 把自己的完成事件与此前最近取件事件关联，回顾实际投递耗时及迟到情况。 |
| [i55](/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_templates.py:675) | self_decision_data_readiness | readiness | snapshot_with_config | 检查自身实体、快照新鲜度及容量/速度配置是否齐备，返回可解释的决策数据缺口。 |

### 执行过的操作

- 阅读 `lade_llm_context_queries.py` 的全部基础查询、参数组装、选择、执行、日志与并发入口。
- 阅读 `lade_llm_context_templates.py` 的扩展查询、共享片段、时间分类、查询包、图结构白名单与校验。
- 阅读 `lade_llm_context_queries_stages.py` 的计时字段，以及现有单元/集成测试的相应语义断言；没有运行这些测试，也不将存在测试代码等同于本轮真实图谱验证通过。
- 通过当前工作区 `./run_in_lade.sh python -B -c '…'` 使用标准库AST提取56条元数据和时间分类，临时清单为 `/tmp/lade_i_query_inventory_20261110.json`。
- 未导入执行压测模块、连接Neo4j或改变服务。
- 本次补充对照还读取 `backend/query_c/catalog.py`、`client.py`、`backend/wide/sim.py` 的能力接口和 `backend/global_writer.py` 的有界候选快照，区分现有业务事实、测量返回与待补数据。

### 修改内容

- 仅新增本文分析，没有修改LaDe、仓储查询或数据库。

### 后续建议

- 先将仓储的每一种待执行操作，映射到“自身事实、任务约束、资源/同伴候选、风险估算、历史反馈、数据质量”这六类问题，再决定需要哪些查询。
- 编写仓储查询时同时定义“返回结果能说明什么、不能说明什么”，尤其是时间、候选、资源预约和动作许可的区别。
- 本轮分析无需用户确认；尚未进行查询迁移或接入机器人决策。

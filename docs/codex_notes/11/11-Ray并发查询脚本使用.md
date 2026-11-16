# Ray 并发查询脚本

新入口：[scripts/warehouse_llm_context_queries_ray.py](../../../scripts/warehouse_llm_context_queries_ray.py)，参考 `/data/zc/datasets/LaDe/viewer/scripts/lade_llm_context_queries_ray.py` 实现。新增脚本和测试，不修改现有线程版、进程池版、14条查询目录或查询读取模块。

## 100个 Actor 的启动命令

将 `test_110404` 换成已经写入完成 Step 的实际 runId：

```bash
./run_in_lade.sh python scripts/warehouse_llm_context_queries_ray.py \
  --run-id test_110404 --robot-ids 1-100 --queries all \
  --max-workers 100 --repeat 5
```

默认100个 Actor、每 Actor 预留1个 Ray CPU资源，分别绑定不同物理核心选出的逻辑 CPU。没有指定机器人编号时自动使用1至 Actor 数。100台机器人、repeat=5，计划500份 context；不是每个 Actor 启动100个并发任务。

CPU选择会读取当前进程允许的 affinity 和 `/sys/devices/system/cpu` 拓扑，自动跳过同一物理核心的 SMT 兄弟线程。100个 Actor 需要当前允许范围内至少100个不同物理核心，不足时明确报错，可降低 `--max-workers`。可用 `--cpu-ids` 手动指定，数量必须与 Actor 数一致且属于不同物理核心。

`num_cpus=1` 是 Ray 资源预留，affinity 约束 Actor 线程执行位置；这些操作不会排除其他系统进程或 Ray 后台服务使用相同核心，日志标记 `system_cpu_exclusivity=false`。

## 与 LaDe 对齐的执行流程

1. 建立独立的本地 Ray runtime（`address=local`），使用独立 `/tmp/whq-ray-*` 目录；不连接现有仿真集群。当前 Python 已连接 Ray 时拒绝复用，避免清理阶段误关闭调用者的 runtime。
2. 每个 Actor 在自己的进程中绑定所有现有线程，创建并校验独立的 Neo4j Driver；后续查询复用该 Driver。每 Actor 的连接池默认上限1。
3. 收集全部 Actor 的 ready 信息，核对独立 PID 和 CPU affinity；全部就绪后才进入查询阶段。
4. 每个 Actor 同时只保留一份 context 请求。主进程通过 `ray.wait()` 收集结果，并给完成工作的 Actor 分配下一份任务，不一次性向 Actor 排队数百次调用。
5. 每份 context 使用当前 warehouse 查询逻辑：固定 Run/tick/数据集边界，新建只读 Session，逐条自动提交。单条查询错误按现有规则记录后继续；缺少锚点或任务输入仍跳过依赖查询。
6. 最后关闭 Driver、终止本次创建的 Actor 并关闭本次本地 runtime。原有脚本文件和其他仿真服务不参与清理。

`repeat` 计数和原版相同：每台机器人生成N份 context，按队列动态分配给 Actor；机器人与 Actor 不固定绑定，不要求各轮同步。

查询 RPC 异常或 Actor 死亡会记录失败任务并停用该 Actor；其他正常 Actor 继续处理。没有可用 Actor 时将尚未分配任务标记失败，不自动重复可能已经发出的查询。Actor 启动/绑定/连接校验失败时，不进入查询阶段，仍执行本次资源清理并保存失败摘要。

## 常用参数

连接参数支持 `--uri / --user / --database / --password`，以及 `NEO4J_* / WAREHOUSE_NEO4J_*`。Ray入口已在代码中内置默认密码 `neo4j123456`，不再交互询问；非空命令行密码、环境变量仍可覆盖默认值，优先级为命令行、`NEO4J_PASSWORD`、`WAREHOUSE_NEO4J_PASSWORD`、代码默认值。日志不保存密码。

```bash
# 不启动Ray、不连接数据库：查看14条语句、Actor数量和任务计划。
./run_in_lade.sh python scripts/warehouse_llm_context_queries_ray.py \
  --run-id test_110404 --queries all --repeat 5 --dry-run

# 只查看查询目录，不需要runId、密码或运行中的Ray。
./run_in_lade.sh python scripts/warehouse_llm_context_queries_ray.py --list-queries

# 两个Actor测试读取：每台重复3次，固定已经发布的tick100。
./run_in_lade.sh python scripts/warehouse_llm_context_queries_ray.py \
  --run-id test_110404 --robot-ids 1-2 --max-workers 2 --repeat 3 \
  --tick 100 --queries core --print-context \
  --output-dir /tmp/warehouse-ray-contexts-001
```

`--startup-timeout` 默认180秒，限制等待全部 Actor 就绪的时间；`--transaction-timeout` 默认30秒，作用于每条自动提交查询；`--max-connection-pool-size` 默认1，是每个 Actor 的上限。还支持 `--task-id / --task-ids / --max-tasks / --history-ticks / --param / --include-cypher / --explain / --progress-every`。

`--dry-run` 不启动 Ray，也不实际验证 CPU 拓扑和数据库。正式运行会检查物理核数量及绑核条件。未指定 `--tick` 时，各 context 分别读取最新完成步；需要同一逻辑时间的比较时应显式指定已发布 tick。

## 日志和耗时

默认目录：`data/warehouse_context_queries_ray/时间戳-随机后缀/`。`--output-dir` 必须指向全新目录。三个文件均由主进程统一写入：

- `contexts.jsonl`：查询事实、缺项、`context_index / robot_id / repeat_index / worker_pid / worker_index / cpu_id / affinity_cpus`。
- `queries.jsonl`：实际查询参数、成功/失败/跳过、行数、耗时、PID与CPU信息，`--include-cypher` 时附完整语句。
- `summary.json`：查询统计，以及 `worker_info / started_worker_count / observed_worker_count / worker_pids / cpu_ids / run_error / cleanup_errors`。

`started_worker_count` 是已返回 ready 元数据的 Actor 数；`observed_worker_count` 是实际返回过 context 的 PID 数。失败 RPC 的执行 PID和查询耗时未知时保留为未知，另外记录原先分配的 Actor 信息，不伪造已测耗时。

总耗时拆分为 `ray_startup_ms`（runtime及Actor初始化、绑核和连接校验）、`query_phase_wall_ms`（并发查询及结果写入）、`ray_cleanup_ms`（关闭连接、Actor和runtime）。每条查询仍报告自己的 `elapsed_ms`，每份 context 报告查询组装时间、`queue_ms` 和 `worker_elapsed_ms`。Ray服务日志保留在摘要指定的独立 `ray_temp_dir` 中用于排查。

## 终端最终汇总

正常运行结束后，终端参照 LaDe Ray 的格式显示可读汇总，不再直接打印整个摘要 JSON：Actor 数、数据库/runId、context 数、实际尝试的查询数、成功/失败/跳过、总耗时、吞吐量、单查询 avg/p95/p99、单 context avg/p95，以及按 `w0～w13` 数字顺序排列的逐语句统计。末尾列出三个日志文件路径；启动或清理失败时也显示错误。

统计口径：总查询数为成功加失败，跳过单列且不计入吞吐；吞吐为尝试执行的查询数除以包含 Ray 启动/清理的总墙钟时间，与 LaDe 的口径对应。延时均值和分位数只使用成功查询；失败 RPC 没有测得的 context 耗时不参与 context 均值/分位数。无有效样本显示 `N/A`。context 计时仍沿用 warehouse 的查询与组装范围，不包含 Ray 启动/清理。

完整 `summary.json` 继续保存，保留原字段，并增加 `context_count / total_queries / success_count / failure_count / skipped_count / throughput_qps / query_avg_ms / query_p95_ms / query_p99_ms / context_avg_ms / by_query / by_category` 等 LaDe 风格统计。终端和新增 `by_query` 使用成功查询延时口径；原 `queries` 分组字段作为兼容信息保留。`--dry-run` 仍输出离线计划 JSON，`--print-context` 仍可打印逐份上下文。

## 验证

新增测试：[tests/test_warehouse_context_ray.py](../../../tests/test_warehouse_context_ray.py)。

```bash
# 14项本地测试；真实Ray测试默认跳过。
./run_in_lade.sh python -B -m unittest -v tests.test_warehouse_context_ray

# 显式运行真实双Actor测试，图谱使用模拟驱动，不写入Neo4j。
WAREHOUSE_CONTEXT_RAY_TEST=1 ./run_in_lade.sh python -B -m unittest -v \
  tests.test_warehouse_context_ray.RealRayTests
```

14项本地测试覆盖CPU拓扑/SMT校验、全线程绑定、独立连接、逐条自动提交、就绪同步、单 Actor 在途限制、repeat、Actor失效及清理，以及最终汇总的成功/失败/跳过口径、延时分位数、JSON保存和终端展示。

真实双 Actor 验证已通过：两个独立 PID、两个不同物理核心、每 Actor 1个 Ray CPU资源，6份 context、12条查询结果，以及两个 Driver 正常关闭。本环境沙箱禁止创建通信 socket，首次测试的 Ray GCS 启动被拒绝；在获准于沙箱外重跑后通过。未连接真实 Neo4j，未执行100 Actor实库压测。

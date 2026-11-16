### 任务背景

用户将仓储仿真页面改为4173端口，希望参照已有LaDe启动函数，把KG/Neo4j环境变量与启动命令封装进用户.bashrc，通过runId和写入模式参数调用。

### 关键结论

- 当前 `/home/chioe/.bashrc` 中对应的LaDe函数为s8010，未找到s8018；保留该函数并新增s4173，位于第208行附近。
- 调用形式为 `s4173 <KG_RUN_ID> [main|ray|process]`；runId必填，省略模式时使用ray。
- 固定连接 `bolt://127.0.0.1:47687`、用户neo4j、数据库warehouse，认证值沿用用户提供的配置；本总结不记录密码。
- 启动参数为4173端口、Ray local、5 CPU、medium19数据集，图谱状态来源neo4j、逐步记录AgentState；继续使用上一轮不写本地SQLite journal的启动器。
- 函数在子shell中切换项目目录并设置临时环境变量，调用结束后保留调用者原工作目录与环境。
- 已检查完整.bashrc语法，并用替身启动脚本验证三种模式、默认模式、参数转发、目录保留和错误输入；实际交互Bash可识别s4173与原s8010。
- 没有启动真实仿真、数据库或执行静态导入；本次仅安装shell函数。

### 执行过的操作

- 读取用户.bashrc的函数、alias和source配置，检查是否存在s4173同名定义。
- 阅读 `ops/start_neo4j_live.py`，确认runId、写入模式、CPU、数据库、端口和数据集参数。
- 在 `/tmp/warehouse-s4173-r3r6_9n7/` 准备函数、候选.bashrc、原文件摘要和安装器。
- 运行 `bash -n` 检查新函数及候选.bashrc。
- 使用临时假项目与替身run_in_lade.sh验证参数，不调用实际Python启动器、不连接数据库。
- 获得工作区外写入授权后，执行安装器，先备份再追加函数；校验原文件摘要避免覆盖准备后出现的新修改。
- 安装后重新验证.bashrc语法；对比备份摘要及安装内容，确认只有预期追加。
- 执行交互Bash的 `type -t s4173`、`type -t s8010`，两者均返回function。

### 修改内容

- `/home/chioe/.bashrc`：追加s4173函数。校验参数数量、runId格式与main/ray/process模式，调用既有 `ops/start_neo4j_live.py`。
- 原文件备份：`/home/chioe/.bashrc.bak-s4173-20261104-110245-698859`。
- 本项目仅新增此总结，未修改仿真、数据集或Neo4j连接代码。

### 后续建议

当前终端先重新加载：

```bash
source ~/.bashrc
```

随后任选一种模式启动，每次使用新的runId：

```bash
s4173 warehouse-test-001 ray
s4173 warehouse-test-002 main
s4173 warehouse-test-003 process
```

不传第二个参数默认ray，例如 `s4173 warehouse-test-004`。这些是备选启动方式，不应在同一个4173端口同时启动多个运行。启动后的页面为 `http://127.0.0.1:4173/`；现有启动器在端口占用时会报错，不会自动停止已有服务。

函数已安装，无待确认事项；启动仿真由用户执行以上命令。

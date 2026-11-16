### 任务背景

确认 warehouse-dashboard 启动时在哪里读取 Neo4j 地址、哪里创建数据库驱动，以及本机当前进程是否配置了连接。

### 关键结论

- 实时入口在 `ops/warehouse_live.py:426` 附近组装图谱查询连接配置；默认 Bolt 地址为 `bolt://127.0.0.1:47687`，用户和数据库默认均为 `neo4j`。
- 查询和机器人写入路径优先读取 `WAREHOUSE_NEO4J_*`，再读取通用 `NEO4J_*`；密码没有内置默认值。
- 查询对象为延迟创建驱动，启动时构造对象不等于已连接；真正请求查询/健康检查时，由 `backend/warehouse_graph_query.py:453` 的 `_get_driver()` 创建驱动，并在后续请求中访问数据库。
- 机器人图谱开关启用时，`backend/ray_medium/sim.py:71` 初始化 schema 和 Run，随后各 actor 的 writer 通过 `backend/warehouse_runtime_graph.py:283` 创建自己的驱动。
- 独立 HTTP 全局写入服务在 `backend/global_writer_api.py:349` 读取配置，该入口只读取 `WAREHOUSE_NEO4J_*`，不回退到通用 `NEO4J_*`。
- 本次发现的两个 live 入口相关进程 PID 4177785、4177837，均未配置上述 URI/用户/数据库环境变量，也没有两种 Neo4j 密码变量；其默认查询配置会指向本机 47687，但缺密码，不能据默认地址认定已经成功连接。
- 之前核对的本地运行只启用了 journal；本次没有建立数据库连接，没有启动或停止任何服务。

### 执行过的操作

- 读取 `ops/warehouse_live.py` 的启动参数和 Neo4jWarehouseGraph 初始化逻辑。
- 检索 `ops/warehouse_live_runtime.py`、`backend/warehouse_graph_query.py`、`backend/warehouse_runtime_graph.py`、`backend/ray_medium/sim.py`、`backend/global_writer.py`、`backend/global_writer_api.py`。
- 读取 `ops/start_neo4j.sh`、检索 `ops/start_runtime.sh`，核对项目本地 Neo4j 的位置与默认端口。
- 用 `./run_in_lade.sh python -c ...` 只读检查匹配 live 入口的进程环境；仅输出 URI/用户/数据库与密码是否存在的布尔值，没有读取输出密码内容。

### 修改内容

仅新增本说明文件，没有修改代码、配置、环境变量或实验数据。

#### 配置优先级

| 配置项 | 实时查询/机器人 writer 优先级 |
|---|---|
| URI | `WAREHOUSE_NEO4J_URI` → `NEO4J_URI` → `bolt://127.0.0.1:47687` |
| 用户 | `WAREHOUSE_NEO4J_USER` → `NEO4J_USER` → `neo4j` |
| 密码 | `WAREHOUSE_NEO4J_PASSWORD` → `NEO4J_PASSWORD`；无默认密码 |
| 数据库 | `WAREHOUSE_NEO4J_DATABASE` → `NEO4J_DATABASE` → `neo4j` |

这些配置来自启动进程继承的环境，当前入口没有在此处自动加载项目 `.env` 的逻辑。若使用环境文件，需要启动命令之前在 shell 中显式加载或由外部服务管理器注入。独立 global_writer_api 应完整设置 `WAREHOUSE_NEO4J_*`，不能只设通用变量。

显式启用旧 `--legacy-a-graph` 的镜像路径还有历史配置规则，`ops/warehouse_live_runtime.py` 的 URI 校验读取 `NEO4J_URI`；不应将旧路径规则套到新 actor 图谱路径。

#### 三处关键连接位置

1. **页面图谱查询**：`ops/warehouse_live.py:426` 传入 URI、用户、密码、数据库；`backend/warehouse_graph_query.py:453` 通过 `GraphDatabase.driver(self.uri, auth=(self.user, self.password))` 创建驱动，session 使用配置的 database。
2. **机器人写入**：`backend/ray_medium/sim.py:71` 读取环境并启动图谱准备；`backend/warehouse_runtime_graph.py:283` 为 writer 创建驱动。普通 journal 启动且未启用 robot-graph，不会自动走该路径。
3. **全局记录服务**：`backend/global_writer_api.py:349` 创建 GlobalWriter，`backend/global_writer.py:237` 创建 Neo4j 驱动。`--global-writer-url` 是 HTTP 服务地址，不是 Neo4j Bolt 地址；实际 Neo4j 由服务进程自己的环境决定。

#### 项目专用 Neo4j 服务在哪里

`ops/start_neo4j.sh` 默认尝试启动：

```text
项目根目录/.runtime/warehouse-neo4j-5.26.32
Bolt：bolt://127.0.0.1:47687
浏览器页面：http://127.0.0.1:47474
```

可通过 `WAREHOUSE_NEO4J_HOME` 覆盖安装目录。该脚本用于启动数据库服务；应用连接该数据库仍依赖上述 URI/密码配置。应用不会因为仿真页面成功启动，就自动安装、初始化并连接 Neo4j。

### 后续建议

- 如要连已有数据库，启动前统一配置 `WAREHOUSE_NEO4J_URI/USER/PASSWORD/DATABASE`，并给机器人进程和全局服务相同的目标。
- 如果只想确认连接代码，优先查看 `ops/warehouse_live.py:426` 和 `backend/warehouse_graph_query.py:453`。
- 本次仅确认源码默认值及进程环境，没有验证 47687 上数据库是否运行，也没有验证凭证；无需用户确认任何改动。

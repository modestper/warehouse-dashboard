### 任务背景

用户希望获得在本机建立仓储项目 Neo4j 数据库的命令。本次仅提供可执行步骤，未拉取镜像、创建容器、修改认证或写入数据库。

### 关键结论

- 当前机器有 `/usr/bin/docker` 命令，而项目 `.venv`、`.runtime` 均不存在；使用 Docker 可以避开项目原生启动脚本对本地 Java 和 `.venv` 的要求。本次未检查 Docker daemon 运行状态。
- 使用 Neo4j Community 5.26 系列镜像，与项目的 5.26 数据导入代码配套；容器名 `warehouse-neo4j`，独立 Docker 数据卷 `warehouse-neo4j-data`。
- 将容器 HTTP 7474、Bolt 7687 映射为本机 47474、47687，与项目默认连接地址一致，仅绑定本机回环地址。
- 用户指定数据库名为 `warehouse`。空实例首次启动通过 `NEO4J_initial_dbms_default__database=warehouse` 创建该数据库，无需额外执行 CREATE DATABASE；用户名仍为 `neo4j`。创建容器后还需要导入 medium 静态仓储数据。
- 导入使用项目已有 `scripts/import_warehouse_neo4j.py --apply`；默认参数已经选择 medium 高峰包和对应地图，但下文仍显式指定以便核对。
- 建库和静态导入不等于启用仿真运行态写图；原 journal 仿真命令仍只保存本地数据。
- 官方文档说明 NEO4J_AUTH 用于空数据卷初始化；已有数据卷的认证不会因重新设置 NEO4J_AUTH 自动改变。

### 执行过的操作

- 查看《启动与图谱数据操作手册》首次环境、Neo4j 安装和 medium 静态导入章节。
- 读取 `ops/start_neo4j.sh` 和 `scripts/import_warehouse_neo4j.py` 参数与导入代码。
- 检查 `docker`、`java` 命令是否存在和项目 `.venv`、`.runtime` 目录状态；未调用 Docker 创建操作。
- 检索 Neo4j 官方 Docker 入门与配置文档，核对端口、数据持久化和初始认证参数。

### 修改内容

仅新增本操作说明，没有修改现有文件或服务。

#### Step 1：准备连接变量并创建本地实例

以下在同一个终端中依次运行。密码至少 8 个字符，交互输入时终端不回显。

```bash
cd /data/zc/datasets/human_robot/warehouse-dashboard

export WAREHOUSE_NEO4J_URI='bolt://127.0.0.1:47687'
export WAREHOUSE_NEO4J_USER='neo4j'
export WAREHOUSE_NEO4J_DATABASE='warehouse'
read -rsp '设置 Neo4j 密码（至少 8 个字符）：' WAREHOUSE_NEO4J_PASSWORD
echo
export WAREHOUSE_NEO4J_PASSWORD

docker run -d \
  --name warehouse-neo4j \
  --restart unless-stopped \
  -p 127.0.0.1:47474:7474 \
  -p 127.0.0.1:47687:7687 \
  -v warehouse-neo4j-data:/data \
  -e NEO4J_AUTH="neo4j/$WAREHOUSE_NEO4J_PASSWORD" \
  -e NEO4J_initial_dbms_default__database=warehouse \
  neo4j:5.26
```

上述命令由 Docker 在需要时创建命名数据卷，数据存放在该卷内，不在项目 data/examples 中。镜像随 5.26 系列补丁更新；正式可复现实验可记录实际 image digest。

`docker run` 用于首次创建。已经有同名容器时，不要删除重建，先查看 `docker ps -a --filter name=warehouse-neo4j`；后续用 `docker start warehouse-neo4j` 启动。

#### Step 2：等待启动并验证

```bash
docker logs --tail 30 warehouse-neo4j
```

等待日志出现 `Started.`，再验证；cypher-shell 会提示输入上面设置的密码：

```bash
docker exec -it warehouse-neo4j cypher-shell \
  -u neo4j -d warehouse 'RETURN 1 AS connected;'
```

返回 connected=1 表示容器内数据库可用。宿主机项目的后续导入会使用 `bolt://127.0.0.1:47687`，同时验证本机映射路径。

Neo4j Browser：`http://127.0.0.1:47474`，连接使用 `bolt://127.0.0.1:47687`，用户 neo4j、数据库 warehouse。这里本地指当前服务器；如果浏览器在另一台机器上，需要将服务器端口转发到浏览器机器，本说明没有开放外网监听。

`NEO4J_initial_dbms_default__database` 仅在空实例首次初始化时生效。已经按之前命令初始化为 neo4j 的数据卷，不会因修改该变量自动改名；需要先核对已有容器和数据，再制定迁移方案。本次没有删除或重建任何容器/数据卷。

#### Step 3：导入当前 medium 静态数据

继续在 Step 1 的同一个终端运行，保留环境变量：

```bash
./run_in_lade.sh python scripts/import_warehouse_neo4j.py \
  --package-dir data/examples/wheeled-medium-peak \
  --layout data/layouts/expansion-candidates/medium/layout.json \
  --locations data/layouts/expansion-candidates/medium/task-locations.csv \
  --apply
```

这会导入 120×90 米 medium 布局、48 个货架、2304 个物理货位及其他位置节点、864 件初始库存对应的货物数据、1728 个入出库任务及相关关系。货物节点还包括任务中新出现的货物，不能把初始库存数当成最终 Goods 节点总数。Location 还包含收发点及逻辑边界，不能把物理货位容量当成 Location 总数。

导入脚本先验证输入摘要和地图身份，再建立约束并批量 MERGE。省略 `--apply` 只做离线检查，不会写库。

在 Browser 执行可查看导入的标签数量：

```cypher
MATCH (n)
RETURN labels(n) AS labels, count(*) AS count
ORDER BY count DESC;
```

#### 日常启停与仿真接入

```bash
docker stop warehouse-neo4j
docker start warehouse-neo4j
```

数据在命名卷中保留。新终端启动仿真或全局服务前，需要重新设置或加载 WAREHOUSE_NEO4J_URI、USER、PASSWORD、DATABASE；当前仓储入口不会自动读取项目 .env 文件。

仿真运行态写图还要启用机器人 writer 与全局服务；仅完成上述静态导入，原 `--knowledge-backend journal` 且没有图谱开关的运行不会自动把每一步写入 Neo4j。启用新完整路径的参数见本目录《03-数据选择与知识图谱写入机制.md》。

### 后续建议

- 先创建空实例并成功导入 medium 静态数据，再单独配置 Ray/全局记录服务运行态写入。
- Docker daemon 不可用时，先解决 Docker 服务或访问权限；本次未验证 daemon，也未实际执行上面的创建与导入命令。
- 参考：[Neo4j 官方 Docker 入门](https://neo4j.com/docs/operations-manual/current/docker/introduction/)、[Docker 配置](https://neo4j.com/docs/operations-manual/current/docker/configuration/)。
- 数据库命名参考：[Docker 参数映射](https://neo4j.com/docs/operations-manual/current/docker/ref-settings/) 与 [首次默认数据库配置](https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/)。数据库初始化名与项目连接时的 database 参数必须同时设置为 warehouse。

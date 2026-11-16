# 独立 Neo4j 容器按 runId 复制

补充：用户后续已指定 `million-110902`，源地址和库名已确认。真实源库核对结果及一条命令入口见 [million-110902 复制准备](10-million-110902复制准备.md)。下文保留最初通用分步说明。

一键入口已支持自定义新库名：`bash ops/copy_neo4j_run_container.sh million-110902 warehouse-copy`，省略第二参数则使用 neo4j。见 [自定义库名的参数与用法](10-复制脚本支持自定义目标库名.md)。

### 任务背景

用户希望新建一个 Neo4j Community 容器，将原容器内指定 runId 的节点和关系复制进去，保留源实例。本次准备了复制脚本和完整操作步骤，没有创建容器或连接真实数据库复制；用户尚未提供具体 runId，当前账号也无 Docker 守护进程访问权限。

### 关键结论

- 新容器应使用独立数据卷、独立端口，业务库仍可叫 neo4j；不共用源 /data，不对源容器执行 stop、rename、删除或 dump。
- 新增 `scripts/copy_neo4j_run.py`，不依赖 APOC，源库只执行 MATCH/SHOW，以 Bolt 分批传递原生属性值。默认仅做只读计划，显式 --apply 才向目标写入。
- 筛选节点为 `n.run_id=指定值`，以及 `:Run {id:指定值}` 根节点。只复制两端都在这个集合中的关系；其他 run 不导入。关系方向、类型、属性、平行关系和自环均保留。
- 共享静态 Dataset、WarehouseTask、地图等没有该 run_id 的节点默认不复制，关联它们的跨界关系也不复制；计划和完成报告会列出被排除的跨界关系类型/数量。这不是原业务查询所需全部静态背景的一键打包。
- 目标库必须为空；不会清空、覆盖或合并已有数据。通过临时节点标识映射重建关系，验证完成且 schema 建成后移除临时标记。失败时保留目标部分副本供检查，源数据不删除。
- 默认重建与选中节点标签/关系类型有关的源约束和独立索引，并等待 ONLINE；默认 lookup 索引由新实例提供。可用 --no-copy-schema 明确跳过，但性能对比应保留一致的索引配置。
- 6 项离线合同测试通过，覆盖混合 run、无 run_id 的根节点、跨界关系、平行边/自环、原生 Date/字节/数组属性、非空目标拒绝、源变化拒绝和 schema 筛选。本次没有真实 Neo4j 端到端验证，不声称数据已复制成功。

### 执行过的操作

- 阅读项目 AGENTS.md、运行态/静态图谱定义、检查点模型和连接配置。
- 尝试 `docker ps --format ...`，沙箱内外均返回 Docker socket permission denied；没有执行容器创建、删除或停止。
- 只读检查计划使用的端口 48474 与 48687，本次检查时没有监听者；实际创建时仍以 Docker 绑定结果为准。
- 新增复制脚本及离线测试，执行：

```bash
./run_in_lade.sh python -B -m unittest -v tests.test_copy_neo4j_run
./run_in_lade.sh python -B scripts/copy_neo4j_run.py --help
git diff --check
```

- 6 项测试通过，耗时 0.993 秒；帮助与差异空白检查通过。
- 查询 Neo4j 官方 Docker、Python Driver 和索引说明。未访问用户真实图数据。

### 修改内容

- `scripts/copy_neo4j_run.py`：按 runId 复制到空库；默认计划、分批写入、目标非空保护、源前后计数/头水位核对、节点/关系及标签/类型数量核对、相关 schema 重建、审计报告。
- `tests/test_copy_neo4j_run.py`：使用离线图模型验证复制合同。该模型不验证真实 Neo4j 的语法、磁盘/网络行为，不能替代实际数据库验收。
- 本文：容器创建和运行步骤。
- 原数据库、现有实验和工作区其他修改保留。

### 后续建议

#### 1. 先选定源 run，并确保它停止写入

在源库 Browser 查看可用运行：

```cypher
MATCH (r:Run)
RETURN r.id AS run_id, r.latest_step AS latest_step
ORDER BY r.started_at_ms DESC;
```

正在运行的 run 应先从其页面暂停，并等待图谱写入队列排空；历史已完成且无人继续修改的 run 更适合复制。不能用“一个只读事务”代替快照隔离：Neo4j 默认 read-committed，扫描期间仍可能看到并发变化。

脚本复制前后比较所选 run 的节点/内部关系/跨界关系计数及 Run/Checkpoint 头水位，能发现普通持续写入。但这不是逐属性快照证明，手工修改属性而保持计数和头水位不变可能无法发现，因此整个复制期间源 run 必须保持不变。

#### 2. 在你有 sudo 权限的终端创建新容器

先列出源容器实际名称：

```bash
sudo docker ps --format '{{.Names}}  {{.Image}}  {{.Ports}}'
```

确认源容器属于你要复制的实例，再在同一个终端执行。代码使用源镜像的本地 image ID，以保持 Neo4j 二进制版本一致；不会继承源数据卷或密码。

```bash
cd /data/zc/datasets/human_robot/warehouse-dashboard

read -rp '源 Neo4j 容器名：' copy_source_container
read -rp '要复制的 runId：' copy_run_id
read -rsp '源 Neo4j 密码：' SOURCE_NEO4J_PASSWORD
echo
read -rsp '新目标 Neo4j 密码（至少8位）：' TARGET_NEO4J_PASSWORD
echo
export SOURCE_NEO4J_PASSWORD TARGET_NEO4J_PASSWORD

copy_image=$(sudo docker inspect --format '{{.Image}}' "$copy_source_container")
copy_stamp=$(date +%Y%m%d-%H%M%S)
copy_container="warehouse-run-copy-$copy_stamp"
copy_volume="warehouse-run-copy-data-$copy_stamp"
export NEO4J_AUTH="neo4j/$TARGET_NEO4J_PASSWORD"

sudo --preserve-env=NEO4J_AUTH docker run -d \
  --name "$copy_container" \
  --restart unless-stopped \
  --memory 3g --cpus 2 \
  -p 127.0.0.1:48474:7474 \
  -p 127.0.0.1:48687:7687 \
  --mount "type=volume,source=$copy_volume,target=/data" \
  --env NEO4J_AUTH \
  --env NEO4J_server_memory_heap_initial__size=512m \
  --env NEO4J_server_memory_heap_max__size=1g \
  --env NEO4J_server_memory_pagecache_size=512m \
  "$copy_image"

unset NEO4J_AUTH
sudo docker logs --tail 30 "$copy_container"
```

等待日志显示 `Started.`。示例给目标容器 3GB 内存上限、2 个逻辑 CPU 配额、1GB JVM heap 和 512MB page cache，避免新实例按整机规模取用资源；这些参数不是已与源库对齐的基准配置。如果源镜像是定制镜像而非标准 Neo4j Community，需要检查其入口行为；上述示例以标准镜像兼容环境为前提。新数据卷不能换成源数据卷。

容器名和数据卷名带时间戳；如果复制失败，不要把目标部分副本当成完整结果，不要重用非空目标重跑。脚本不会替你删除任何容器或数据卷。

#### 3. 先查看只读计划

以下假定源地址仍为 bolt://127.0.0.1:47687、业务库 neo4j，目标为新容器的 neo4j。若源是 warehouse 或其他实例，应修改源库名/地址。

```bash
./run_in_lade.sh python scripts/copy_neo4j_run.py \
  --run-id "$copy_run_id" \
  --source-uri bolt://127.0.0.1:47687 \
  --source-database neo4j \
  --target-uri bolt://127.0.0.1:48687 \
  --target-database neo4j \
  --report "/tmp/neo4j-run-copy-$copy_stamp-plan.json"
```

密码从上一步的两个环境变量读取；未设置时脚本交互输入，不回显、不写报告。报告不覆盖已有文件，重试时应换新报告名。

计划中的 nodes/relationships 是将要复制的运行子图规模。cross_boundary_relationships 是默认排除的关系，例如 USES_DATASET、EXECUTES 等。如果你的目标查询需要这些共享静态节点，应先明确扩展范围，不能把运行子图误认为与源库全部查询语义相同。

#### 4. 执行复制

确认计划范围正确，源 run 在整个复制过程中保持不变后，在同一终端执行：

```bash
./run_in_lade.sh python scripts/copy_neo4j_run.py \
  --run-id "$copy_run_id" \
  --source-uri bolt://127.0.0.1:47687 \
  --source-database neo4j \
  --target-uri bolt://127.0.0.1:48687 \
  --target-database neo4j \
  --batch-size 500 \
  --report "/tmp/neo4j-run-copy-$copy_stamp-result.json" \
  --apply
```

脚本使用源库只读 session 和 MATCH/SHOW；只在目标创建节点、关系、约束、索引和临时映射。标签/关系类型进行标识符转义，属性直接经 Bolt 保留原生类型；节点 internal elementId 会重新分配，业务 id/run_id 保持。

成功报告 status=complete，包含复制节点数、关系数、标签/类型分布和创建的 schema 名称。没有该成功报告时，不应把目标作为完整副本进行基准实验。

最后可清除当前终端的密码变量：

```bash
unset SOURCE_NEO4J_PASSWORD TARGET_NEO4J_PASSWORD
```

#### 5. 查询目标副本

在服务器本机打开 `http://127.0.0.1:48474/browser/`，连接地址 `bolt://127.0.0.1:48687`，使用新密码。若从自己电脑访问，建立本地转发：

```bash
ssh -N -L 48474:127.0.0.1:48474 -L 48687:127.0.0.1:48687 用户名@222.20.99.83
```

再在自己电脑打开同一 localhost 页面和 Bolt 地址。若本地端口已占用，应相应修改转发命令左侧端口及 Browser 连接地址。

在目标选择 neo4j 后执行：

```cypher
MATCH (n) RETURN count(n) AS nodes;
```

```cypher
MATCH ()-[r]->() RETURN count(r) AS relationships;
```

```cypher
SHOW INDEXES;
```

应与成功报告中的规模一致，索引为 ONLINE。做性能对比时还需对齐容器 CPU/内存、页缓存、索引和预热条件；两个容器仍共享宿主机资源。

参考：[Neo4j Docker](https://neo4j.com/docs/operations-manual/current/docker/introduction/)、[Python Driver 查询机制](https://neo4j.com/docs/python-manual/current/query-advanced/)、[索引列表与状态](https://neo4j.com/docs/cypher-manual/current/indexes/performance-indexes/list-indexes/)。

本次缺少指定 runId，且实际 Docker 权限不足，因此没有执行创建或复制。若要继续由助手操作，需要提供 runId/源地址确认，并具备 Docker 访问条件；不需要重复授予已经明确授权的复制范围。

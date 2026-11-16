# Neo4j 整库复制 A 到 B

### 任务背景

用户希望了解如何把 database A 的数据完整复制到 database B。本次查询官方文档并整理示例，没有连接数据库、停止服务或执行复制。

### 关键结论

- 整库复制可采用 `neo4j-admin database dump` 导出 A，再用 `neo4j-admin database load` 加载为 B；这属于整库恢复，不是向已有 B 追加合并。
- dump 要求源库离线。Enterprise 可在 DBMS 继续运行时停止对应数据库操作；Community 要求整个 DBMS 离线。
- 同一个实例中并存两个标准业务数据库需要 Enterprise；Community 仅支持一个标准业务库。system 是管理库，不能当作第二个业务库来装 A 的数据。
- Enterprise 的新 B 在 load 完成后还需 `CREATE DATABASE` 注册并启动。已有 B 的替换必须停止 B，并涉及 overwrite-destination；本文默认不覆盖，不提供覆盖式执行指令。
- 工具必须作用于正确的 Neo4j 数据目录，并使用兼容版本和正确文件权限。Docker 部署需要同一数据卷上的管理容器或相应工具环境，不能随意在不挂载数据卷的容器中执行。
- 官方支持从标准输入指定源 archive，因此不必把 A.dump 改名成 B.dump 就可以加载为 B。
- 下列步骤仅适用于 Enterprise 单机、新 B 的离线整库复制示例；不适用于集群、Aura、需要不停机复制或向已有 B 合并的场景。

### 执行过的操作

- 阅读官方离线 dump、restore dump、Docker dump/load 和数据库管理说明。
- 尝试访问旧 `/5/` 操作手册路径，本次网络工具无法打开；最终引用可访问的 current 官方手册。示例采用 Neo4j 5.x 起使用的基础 dump/load 命令形式，具体安装应先检查本机 `--help`。
- 未运行 `STOP DATABASE`、dump、load、`CREATE DATABASE`、Docker 或任何数据库写入命令。

### 修改内容

- 仅新增本文说明，没有修改仓储项目代码或数据库内容。

### 后续建议

#### 先确认版本与 edition

在 Neo4j Browser 中执行以下只读命令：

```cypher
CALL dbms.components()
YIELD name, versions, edition
RETURN name, versions, edition;
```

```cypher
SHOW DATABASES;
```

如果只有 neo4j 与 system，不能据此认为可以把业务库复制到 system。

#### Enterprise 单机：源 dba → 新库 dbb

以下用合法示例库名 dba 和 dbb 表示 A、B，执行前替换成实际名称。dbb 必须尚不存在；dump 目录必须提前存在，且 Neo4j 工具运行用户有访问权限。源 dba 会短暂停机，现有应用需先安排停止向它读写。

1. 在 Browser 的 system 数据库中执行：

```cypher
STOP DATABASE dba WAIT;
```

2. 在服务器终端、对应 Neo4j 安装及数据目录的工具环境中，以合适的 Neo4j 文件拥有者身份执行：

```bash
neo4j-admin database dump dba --to-path=/absolute/path/to/backups
```

成功后得到 `/absolute/path/to/backups/dba.dump`。此时就可以在 Browser 中重新开放源库：

```cypher
START DATABASE dba WAIT;
```

3. 在目标实例的工具环境中加载到新 B：

```bash
neo4j-admin database load dbb --from-stdin < /absolute/path/to/backups/dba.dump
```

若 A/B 同实例，就是同一套工具环境。不同实例则先把 dump 文件复制到目标服务器。没有加 overwrite-destination，避免把已有目标库覆盖。

4. 在目标实例 Browser 的 system 库中注册 B：

```cypher
CREATE DATABASE dbb WAIT;
```

不要使用 CREATE OR REPLACE DATABASE，它会删除已有目标库。

5. 分别选中 A 和 B，对比节点数、关系数、关键标签、索引和约束状态：

```cypher
MATCH (n) RETURN count(n) AS nodes;
```

```cypher
MATCH ()-[r]->() RETURN count(r) AS relationships;
```

```cypher
SHOW INDEXES;
SHOW CONSTRAINTS;
```

如果源 A 在 dump 后恢复写入，B 是导出时的快照，之后两库不自动同步；对比时应选定一致的时间点，不能要求继续增长的 A 始终与 B 数量相同。

#### Community 或 Docker 情况

- Community 同一实例不能并存 A/B 两个标准业务库。保留 A 的同时复制出 B，应使用另一个 Neo4j 实例/容器和独立数据卷；每个实例可各自使用 neo4j 作为业务库名。
- Community 的 dump/load 必须在对应实例离线时执行。Docker 下停止服务容器后，一般通过挂载源/目标数据卷与备份目录的管理容器执行命令。所用镜像版本应与源/目标部署兼容，不能直接照搬官方 current 示例的版本号。
- 如果不能停源库，Enterprise 可考虑在线 backup，再在目标恢复；这需要额外确认部署与备份配置。
- 如果 B 已有需要保留的数据，需求变成合并导入，不能用整库 load 直接追加。

官方资料：

- [离线导出 dump](https://neo4j.com/docs/operations-manual/current/backup-restore/offline-backup/)
- [加载 dump，含标准输入指定 archive 示例](https://neo4j.com/docs/operations-manual/current/backup-restore/restore-dump/)
- [数据库与 edition 限制](https://neo4j.com/docs/operations-manual/current/database-administration/)
- [Docker 离线导出与导入](https://neo4j.com/docs/operations-manual/current/docker/dump-load/)

本文为操作说明，没有待确认的已准备数据库变更。若后续要求实际执行，应先确定源/目标实例、库名、edition、部署方式及 B 是否已有数据，才能安排源库停机或目标替换。

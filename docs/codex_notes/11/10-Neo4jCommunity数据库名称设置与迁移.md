# Neo4j Community 数据库名称设置与迁移

后续更新：复制入口现已支持第二个参数指定新业务库名，例如 `bash ops/copy_neo4j_run_container.sh million-110902 warehouse-copy`。创建容器、就绪检查、复制目标与连接提示会统一使用该名称。详见 [复制脚本支持自定义目标库名](10-复制脚本支持自定义目标库名.md)。以下迁移原理仍适用。

### 任务背景

用户希望了解如何更改 Neo4j 数据库名称。上下文为 Neo4j Community 5.26.32、Docker 部署，以及按 runId 复制到独立容器的流程。本次只查阅官方说明并整理操作方法，没有修改脚本、容器、配置或数据库。

### 关键结论

- Docker 容器名与 database 名是不同概念。docker rename 只改变容器名称，不改变 Neo4j 内部业务库名。
- 对尚未初始化的全新数据卷，可以通过 `NEO4J_initial_dbms_default__database=warehouse-copy` 指定初始业务库名，默认值才是 neo4j。
- initial.dbms.default_database 仅在首次 DBMS 初始化前生效。对已初始化的数据卷修改该环境变量，不能把现有 neo4j 重命名为 warehouse-copy。
- Community 同实例只有一个标准业务库。已有数据需要保留时，应安排离线 dump/load，以新名称装入新的数据卷/实例，再用相同初始库名启动，不要手动重命名数据库文件夹或删除旧卷。
- 创建容器、复制工具的 --target-database、就绪检查以及 cypher-shell 的 -d 必须使用同一个库名。改变连接参数本身不会创建或重命名数据库。
- 最初版本的 `ops/copy_neo4j_run_container.sh` 就绪检查和复制目标默认写为 neo4j；后续已统一接入可选第二参数。基础复制脚本支持显式 --target-database。

### 执行过的操作

- 查阅 Neo4j 官方 configuration settings、Docker 配置名称映射和数据库管理文档。
- 对照上下文中的当前容器复制脚本和基础复制工具，区分初始化、复制目标和连接参数。
- 未执行停库、dump/load、容器重建、数据卷修改或数据库写入。

### 修改内容

- 仅新增本文操作说明，没有修改当前运行或现有脚本。

### 后续建议

#### 一、还没初始化的新容器

假设希望业务库叫 warehouse-copy，在新容器的 docker run 中增加：

```bash
--env NEO4J_initial_dbms_default__database=warehouse-copy
```

必须配全新、尚未初始化的 /data 数据卷，并保留其他必要的认证、端口和内存配置。不要把已使用的数据卷换个容器名后当作新卷。

创建成功后，在其 Browser 执行：

```cypher
SHOW DATABASES;
```

应看到 warehouse-copy 和 system。目标端口仍为 48687 时，连接命令为：

```bash
cypher-shell -a bolt://127.0.0.1:48687 -u neo4j -d warehouse-copy
```

用户名 neo4j 与业务库名独立，不必因改库名改变用户名。

如果要把 million-110902 从源复制进去，基础脚本显式选择目标名称：

```bash
./run_in_lade.sh python scripts/copy_neo4j_run.py \
  --run-id million-110902 \
  --source-uri bolt://127.0.0.1:47687 \
  --source-database neo4j \
  --target-uri bolt://127.0.0.1:48687 \
  --target-database warehouse-copy \
  --report /tmp/million-110902-warehouse-copy-plan.json
```

默认只读计划；要执行写入需另用新的报告文件并加 --apply，目标库必须为空，源 run 在整个复制期间应保持不变。这些命令没有执行，目标容器也未由本次操作创建。

#### 二、已经有数据的 neo4j 库

推荐迁移流程：

1. 确认要改名的是哪个实例/副本，并安排其离线；Community 的 dump 要求对应 DBMS 离线。
2. 用该实例匹配版本的 neo4j-admin 导出旧库 neo4j。
3. 在全新目标数据卷的工具环境中加载为 warehouse-copy。
4. 首次启动该新卷的容器时设置初始库名 warehouse-copy。
5. 核对节点数、关系数、关键标签、约束和索引 ONLINE，再决定是否切换业务连接；旧容器和旧卷保留。

工具环境中的核心命令是：

```bash
neo4j-admin database dump neo4j --to-path=/absolute/path/to/backups
```

随后在新数据卷对应的工具环境中执行：

```bash
neo4j-admin database load warehouse-copy --from-stdin \
  < /absolute/path/to/backups/neo4j.dump
```

两条命令不能理解为对当前正在运行的 Community 实例执行在线改名；Docker 下还需挂载正确的旧卷、新卷和备份目录，并确保版本与文件权限匹配。尤其不要停止 47687 的源实例来给另一个副本改名，除非那确实是用户选择的迁移对象。

这里没有使用 overwrite-destination，没有删除现有库或卷。无法通过一条通用 Cypher ALTER DATABASE ... RENAME TO 命令完成当前 Community 场景。

#### 三、命名规则

当前源实例为 Neo4j 5.26.32，名称长度应为 3～63，使用英文字母、数字、点、短横线，首字符为英文字母，不能以 system 开头。脚本同时要求以字母或数字结尾，并统一转为小写。建议使用 warehouse-copy；不要照搬允许下划线的 runId 规则。新版文档允许数字开头的行为从 Neo4j 2025.01 才开始支持，不适用于当前 5.26 源镜像，参见 [Neo4j 5 命名规则](https://neo4j.com/docs/cypher-manual/5/cheat-sheet/) 和 [新版差异说明](https://neo4j.com/docs/operations-manual/current/database-administration/standard-databases/naming-databases/)。

参考：[初始数据库名配置及首次启动限制](https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/)、[Docker 参数名称映射](https://neo4j.com/docs/operations-manual/current/docker/ref-settings/)、[Community 业务库数量限制](https://neo4j.com/docs/operations-manual/current/database-administration/)、[dump 加载为指定名称](https://neo4j.com/docs/operations-manual/current/backup-restore/restore-dump/)。

下一步若要实际迁移，需要确定新名称、目标实例以及是否已经有数据，避免将初始化设置误用于已有数据卷。本次为教学说明，无已执行的数据库变更需要确认。

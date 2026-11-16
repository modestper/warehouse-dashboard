# warehouse 数据库不存在的排查与修复

2026-11-04 实际只读核对了 `bolt://127.0.0.1:47687`：认证成功，Neo4j Community 5.26.32，数据库只有 `neo4j` 和 `system`；业务库 `neo4j` 在线且节点数为0。`s4173` 选择 `warehouse`，因此收到 DatabaseNotFound。

`NEO4J_DATABASE=warehouse` 是客户端选择库名，不会创建库。Docker 的 `NEO4J_initial_dbms_default__database=warehouse` 只在空实例首次初始化时生效，给已经初始化的旧数据卷加这个变量不会自动改名。见 [Neo4j 官方配置说明](https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/)。

## 保留 warehouse 库名的修复命令

以下步骤会短暂停止47687上的仓储实例，保留原容器和原数据卷，再用全新数据卷建立warehouse。适用于本次已核实为空的实例。需要在用户终端输入sudo密码；Codex没有sudo凭据，未执行下面的切换操作。

先确认容器名及端口：

```bash
sudo docker ps --filter name=warehouse-neo4j --format '{{.Names}}  {{.Ports}}'
```

应确认名为warehouse-neo4j的容器对应47687。随后在同一个终端完整执行此代码块；密码输入应与s4173中配置的当前Neo4j密码相同：

```bash
(
  set -e
  warehouse_db_stamp=$(date +%Y%m%d-%H%M%S)
  read -rsp '输入当前 Neo4j 密码：' warehouse_db_password
  echo
  test -n "$warehouse_db_password"

  sudo docker inspect warehouse-neo4j >/dev/null
  sudo docker stop warehouse-neo4j
  sudo docker rename warehouse-neo4j "warehouse-neo4j-backup-$warehouse_db_stamp"

  sudo docker run -d \
    --name warehouse-neo4j \
    --restart unless-stopped \
    -p 127.0.0.1:47474:7474 \
    -p 127.0.0.1:47687:7687 \
    -v "warehouse-neo4j-warehouse-data-$warehouse_db_stamp:/data" \
    -e NEO4J_AUTH="neo4j/$warehouse_db_password" \
    -e NEO4J_initial_dbms_default__database=warehouse \
    neo4j:5.26
)
```

新卷名带时间戳，避免再次复用已经初始化为neo4j的旧卷。原容器停止后改名为warehouse-neo4j-backup-时间戳，没有执行删除容器或删除卷的命令。

## 验证并重新启动仿真

```bash
sudo docker logs --tail 30 warehouse-neo4j
```

等待日志出现 `Started.`，再验证warehouse库确实可用，按提示输入数据库密码：

```bash
sudo docker exec -it warehouse-neo4j cypher-shell \
  -u neo4j -d warehouse 'RETURN 1 AS connected;'
```

返回1后重新运行：

```bash
s4173 test_1104 process
```

之前失败发生在只读连接检查，未创建仿真run或导入节点，因此该次失败的runId可以再次尝试；如果后续已经启动成功，再次新建运行需要使用新的runId。

## 已完成的代码改进

启动器针对DatabaseNotFound单独处理：显示目标库不存在、列出可见库名、说明不会自动建库，并在导入和启动任何服务之前退出。不会静默换到neo4j库。认证等其他错误保持原有分类，避免误导为库名问题。

如果新容器启动异常，旧容器与旧卷仍在，可从 `sudo docker ps -a` 找到备份名称，保留异常新容器后将旧容器改回原名并启动。不要删除原数据卷。

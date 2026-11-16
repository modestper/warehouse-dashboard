### 任务背景

用户执行 `s4173 test_1104 process` 时，Neo4j返回 `Neo.ClientError.Database.DatabaseNotFound: Graph not found: warehouse`，要求排查启动失败。

### 关键结论

- 已通过只读查询确认47687可连接、认证有效；实际为Neo4j Community 5.26.32。
- 实例只有neo4j和system两个库，没有warehouse；neo4j在线且当前节点数为0。
- 当前s4173正确选择了用户要求的warehouse库，但该库没有在服务端完成初始化；设置客户端NEO4J_DATABASE不会创建库。
- initial.dbms.default_database仅在空实例首次初始化时生效，复用已初始化旧卷不能靠添加环境变量改名。
- 保留warehouse名称的修复方案是保存旧容器/卷，使用新卷并设置首次库名参数；完整可执行命令已写入操作说明。
- Docker socket访问被操作系统权限拒绝；sudo -n也报告需要密码，因而未停止、重建或修改数据库容器。
- 已改进启动器错误提示并通过8项定向测试，且在实际实例上只读验证了新的提示；不会自动回退到其他库。

### 执行过的操作

- 读取ops/start_neo4j_live.py、对应测试、现有git状态。
- 通过run_in_lade.sh使用Neo4j Python驱动查询SHOW DATABASES、dbms.components和节点数；密码从s4173配置读入，未输出。
- 沙箱内socket操作被拒绝后，获准在沙箱外执行只读查询。
- docker ps在沙箱内外均被操作系统拒绝；sudo -n docker ps报告需要密码，未进一步执行Docker修改操作。
- 查阅Neo4j官方首次默认数据库配置说明。
- 执行 `./run_in_lade.sh python -B -m unittest -v tests.test_start_neo4j_live`，8项全部通过。
- 在真实47687实例上只读调用verify_database，确认输出warehouse不存在、可见库为neo4j/system，并且没有写入或回退。

### 修改内容

- ops/start_neo4j_live.py：新增DatabaseSelectionError和verify_database；只针对DatabaseNotFound补充库名诊断，主入口捕获后给出命令行错误而非长堆栈。
- tests/test_start_neo4j_live.py：覆盖缺库列表、无回退、导入/子进程不启动，以及认证类错误不误分类。
- docs/NEO4J_DATABASE_NOT_FOUND.md：完整保留旧容器/数据卷的修复命令、验证和重新启动方法。
- docs/NEO4J_ONLY_START.md：链接缺库排查入口。
- 本总结文件；未改.bashrc、数据库连接目标、容器或卷。

### 后续建议

用户在具备sudo权限的终端，按 `docs/NEO4J_DATABASE_NOT_FOUND.md` 核对容器后执行修复。新实例必须使用新数据卷并设置 `NEO4J_initial_dbms_default__database=warehouse`。等待启动完成后，用cypher-shell显式验证warehouse，再运行s4173。

代码诊断改进已完成；实际建库/切换仍待用户执行sudo命令。本次不声称数据库已修复或仿真已成功启动。

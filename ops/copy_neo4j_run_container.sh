#!/usr/bin/env bash
# Run in a user terminal with Docker access or interactive sudo privileges.
# Creates a fresh target; never stops, deletes, or writes to the source.
set -euo pipefail
if [[ $# -eq 1 && "$1" == '--help' ]]; then
  printf '%s\n' '用法: bash ops/copy_neo4j_run_container.sh <runId> [目标数据库名]' \
    '默认源: bolt://127.0.0.1:47687 / neo4j；目标端口: 48474 / 48687。' \
    '目标数据库名默认 neo4j；示例: bash ops/copy_neo4j_run_container.sh million-110902 warehouse-copy' \
    '库名须为3～63位，以英文字母开头，仅含字母、数字、点和短横线，以字母或数字结尾，不得以 system 开头。' \
    '该参数用于全新数据卷的首次初始化，不会重命名已有库。' \
    '源 run 应已暂停/完成且图谱写入排空。密码交互输入，不写入报告。'
  exit 0
fi
if [[ $# -lt 1 || $# -gt 2 || ! "${1:-}" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$ ]]; then
  printf '%s\n' '用法: bash ops/copy_neo4j_run_container.sh <runId> [目标数据库名]，例如 million-110902 warehouse-copy。' >&2
  exit 2
fi
copy_run_id="$1"
copy_target_database="${2-neo4j}"
# Match the source image's Neo4j 5 naming rules, rather than the broader runId syntax.
if [[ ! "$copy_target_database" =~ ^[A-Za-z][A-Za-z0-9.-]{1,61}[A-Za-z0-9]$ ]]; then
  printf '%s\n' '目标数据库名须为3～63位，以英文字母开头，仅含字母、数字、点和短横线，以字母或数字结尾。' >&2
  exit 2
fi
copy_target_database="${copy_target_database,,}"
if [[ "$copy_target_database" == system* ]]; then
  printf '%s\n' '目标数据库名不能使用 Neo4j 保留的 system 前缀。' >&2
  exit 2
fi
copy_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$copy_root"
copy_container=''
trap 'copy_rc=$?; unset SOURCE_NEO4J_PASSWORD TARGET_NEO4J_PASSWORD NEO4J_AUTH; if [[ $copy_rc -ne 0 && -n "$copy_container" ]]; then printf "操作未完成；目标容器保留以供检查：%s。未删除源数据。\n" "$copy_container" >&2; fi' EXIT
if docker info >/dev/null 2>&1; then
  copy_docker=(docker)
else
  # The password is read by sudo itself from the user's terminal, not by Codex.
  sudo -v
  copy_docker=(sudo -n --preserve-env=NEO4J_AUTH docker)
fi
copy_sources=()
while IFS='|' read -r copy_id copy_ports; do
  if [[ "$copy_ports" == *':47687->'* ]]; then
    copy_sources+=("$copy_id")
  fi
done < <("${copy_docker[@]}" ps --format '{{.ID}}|{{.Ports}}')
if [[ ${#copy_sources[@]} -ne 1 ]]; then
  printf '%s\n' '47687 必须对应唯一运行中的 Docker 容器，请先检查 sudo docker ps。' >&2
  exit 1
fi
copy_image="$("${copy_docker[@]}" inspect --format '{{.Image}}' "${copy_sources[0]}")"
if [[ -z ${SOURCE_NEO4J_PASSWORD:-} ]]; then
  read -rsp '源 Neo4j 密码：' SOURCE_NEO4J_PASSWORD
  printf '\n'
fi
if [[ -z ${TARGET_NEO4J_PASSWORD:-} ]]; then
  read -rsp '新目标 Neo4j 密码（至少8位）：' TARGET_NEO4J_PASSWORD
  printf '\n'
fi
if [[ -z "$SOURCE_NEO4J_PASSWORD" || ${#TARGET_NEO4J_PASSWORD} -lt 8 ]]; then
  printf '%s\n' '源密码不能为空，目标密码至少8位。' >&2
  exit 2
fi
export SOURCE_NEO4J_PASSWORD TARGET_NEO4J_PASSWORD
# Check credentials and run existence before creating any target container.
COPY_RUN_ID="$copy_run_id" ./run_in_lade.sh python -B - <<'PY'
import os
from neo4j import GraphDatabase, READ_ACCESS
with GraphDatabase.driver('bolt://127.0.0.1:47687',
                          auth=('neo4j', os.environ['SOURCE_NEO4J_PASSWORD']),
                          connection_timeout=10, max_transaction_retry_time=0) as driver:
    with driver.session(database='neo4j', default_access_mode=READ_ACCESS) as session:
        row=session.run('MATCH (n) WHERE n.run_id=$run OR (n:Run AND n.id=$run) '
                        'RETURN elementId(n) AS id LIMIT 1',run=os.environ['COPY_RUN_ID']).single()
        if row is None:raise SystemExit('源库未找到指定 runId，尚未创建目标容器。')
PY
copy_stamp="$(date +%Y%m%d-%H%M%S)-$$"
copy_container="warehouse-run-copy-$copy_stamp"
copy_volume="warehouse-run-copy-data-$copy_stamp"
copy_report_dir="$copy_root/data/neo4j-run-copies/$copy_run_id/$copy_stamp"
mkdir -p "$copy_report_dir"
export NEO4J_AUTH="neo4j/$TARGET_NEO4J_PASSWORD"
"${copy_docker[@]}" run -d --name "$copy_container" --restart unless-stopped \
  --memory 3g --cpus 2 \
  -p 127.0.0.1:48474:7474 -p 127.0.0.1:48687:7687 \
  --mount "type=volume,source=$copy_volume,target=/data" \
  --env NEO4J_AUTH \
  --env "NEO4J_initial_dbms_default__database=$copy_target_database" \
  --env NEO4J_server_memory_heap_initial__size=512m \
  --env NEO4J_server_memory_heap_max__size=1g \
  --env NEO4J_server_memory_pagecache_size=512m "$copy_image"
unset NEO4J_AUTH
COPY_TARGET_DATABASE="$copy_target_database" ./run_in_lade.sh python -B - <<'PY'
import os,time
from neo4j import GraphDatabase,READ_ACCESS
from neo4j.exceptions import ServiceUnavailable,SessionExpired,AuthError
with GraphDatabase.driver('bolt://127.0.0.1:48687',
                          auth=('neo4j',os.environ['TARGET_NEO4J_PASSWORD']),
                          connection_timeout=2,connection_acquisition_timeout=2,
                          max_transaction_retry_time=0) as driver:
    for attempt in range(60):
        try:
            with driver.session(database=os.environ['COPY_TARGET_DATABASE'],default_access_mode=READ_ACCESS) as session:
                session.run('RETURN 1 AS ready').consume()
            break
        except AuthError:raise SystemExit('目标认证失败，请检查新容器日志。')
        except (ServiceUnavailable,SessionExpired):time.sleep(2)
    else:raise SystemExit('等待目标 Neo4j 启动超时；目标容器保留，源实例未修改。')
PY
./run_in_lade.sh python -B scripts/copy_neo4j_run.py \
  --run-id "$copy_run_id" --target-database "$copy_target_database" \
  --report "$copy_report_dir/plan.json"
./run_in_lade.sh python -B scripts/copy_neo4j_run.py \
  --run-id "$copy_run_id" --target-database "$copy_target_database" --batch-size 500 \
  --report "$copy_report_dir/result.json" --apply
printf '复制完成。容器：%s\n数据卷：%s\n报告：%s/result.json\n' \
  "$copy_container" "$copy_volume" "$copy_report_dir"
printf 'Browser：http://127.0.0.1:48474/browser/；Bolt：bolt://127.0.0.1:48687；业务库：%s。\n' "$copy_target_database"
printf 'Cypher Shell：cypher-shell -a bolt://127.0.0.1:48687 -u neo4j -d %s\n' "$copy_target_database"

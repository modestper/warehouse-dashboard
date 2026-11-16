"""Import a warehouse100 generated package into the dedicated Neo4j service."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.warehouse_graph import create_schema, import_graph, load_graph_package, validation_report


def parse_args() -> argparse.Namespace:
    from ops.warehouse_kg_config import Neo4jConnection
    config = Neo4jConnection.from_env()
    parser = argparse.ArgumentParser(description="Import the warehouse100 static knowledge graph.")
    parser.add_argument("--package-dir", type=Path, default=Path("data/examples/wheeled-medium-peak"))
    parser.add_argument("--layout", type=Path, default=Path("data/layouts/expansion-candidates/medium/layout.json"))
    parser.add_argument("--locations", type=Path, default=Path("data/layouts/expansion-candidates/medium/task-locations.csv"))
    parser.add_argument("--graph-dataset-id", help="registered static Dataset identity; isolates packages sharing source version/date")
    parser.add_argument("--uri", default=config.uri)
    parser.add_argument("--user", default=config.user)
    parser.add_argument("--database", default=config.database)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--apply", action="store_true", help="actually connect and write; omitted means offline dry-run")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    package = load_graph_package(args.package_dir, args.layout, args.locations, dataset_id=args.graph_dataset_id)
    print(json.dumps({"mode": "apply" if args.apply else "dry-run", **validation_report(package)}, ensure_ascii=False, indent=2))
    if not args.apply:
        return
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise SystemExit("缺少 neo4j 驱动；先运行：./.venv/bin/python -m pip install 'neo4j>=5.26,<6'") from exc
    from ops.warehouse_kg_config import Neo4jConnection
    password = Neo4jConnection.from_env().password or getpass.getpass("Neo4j 密码: ")
    driver = GraphDatabase.driver(args.uri, auth=(args.user, password))
    try:
        driver.verify_connectivity()
        with driver.session(database=args.database) as session:
            create_schema(session)
            summary = import_graph(session, package, args.batch_size)
        print(json.dumps({"imported": summary, "uri": args.uri, "database": args.database}, ensure_ascii=False, indent=2))
    finally:
        driver.close()


if __name__ == "__main__":
    main()

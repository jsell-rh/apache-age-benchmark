"""Strategy 2: COPY to staging + UNWIND MERGE.

This strategy uses PostgreSQL's COPY protocol to rapidly load data
into a temporary staging table, then fetches batches from staging
and uses Cypher UNWIND MERGE to insert into the graph.

COPY is extremely fast for bulk data loading (binary protocol).
"""

from __future__ import annotations

import io
import json
from typing import Any

from psycopg2.extensions import connection as PsycopgConnection

from src.strategies.protocol import BulkInsertStrategy, EdgeData, NodeData


class CopyUnwindStrategy(BulkInsertStrategy):
    """Use COPY to staging table + UNWIND MERGE from staging.

    This combines the speed of PostgreSQL COPY for data loading
    with Cypher UNWIND for graph insertion.

    Performance characteristics:
    - COPY uses binary protocol (very fast)
    - Data loaded to temp table first
    - Fetch batches from staging
    - UNWIND MERGE each batch (still Cypher overhead)
    """

    BATCH_SIZE = 200

    @property
    def name(self) -> str:
        return "2. COPY + UNWIND"

    @property
    def description(self) -> str:
        return "COPY to staging + UNWIND MERGE"

    def _format_value(self, value: Any) -> str:
        """Format Python value for Cypher query."""
        if isinstance(value, str):
            escaped = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif value is None:
            return "null"
        elif isinstance(value, dict):
            items = [f"{k}: {v}" for k, v in value.items()]
            return self._format_value(items)
        elif isinstance(value, list):
            formatted_items = [self._format_value(item) for item in value]
            return f"[{', '.join(formatted_items)}]"
        else:
            return str(value)

    def _build_merge_nodes_query(self, rows: list[dict], label: str) -> str:
        """Build UNWIND MERGE query from staging data."""
        items = []
        all_props: set[str] = set()

        for row in rows:
            props = row["properties"]
            all_props.update(props.keys())

        for row in rows:
            props = row["properties"]
            item_parts = [f"id: '{row['id']}'"]
            for prop in all_props:
                value = props.get(prop)
                item_parts.append(f"`{prop}`: {self._format_value(value)}")
            items.append("{" + ", ".join(item_parts) + "}")

        items_str = ", ".join(items)
        set_clauses = [f"SET n.`{prop}` = item.`{prop}`" for prop in all_props]

        return (
            f"WITH [{items_str}] AS items "
            f"UNWIND items AS item "
            f"MERGE (n:{label} {{id: item.id}}) " + " ".join(set_clauses)
        )

    def _build_merge_edges_query(self, rows: list[dict], label: str) -> str:
        """Build UNWIND MERGE query for edges from staging data."""
        items = []
        all_props: set[str] = set()

        for row in rows:
            props = row["properties"]
            all_props.update(props.keys())

        for row in rows:
            props = row["properties"]
            item_parts = [
                f"id: '{row['id']}'",
                f"start_id: '{row['start_id']}'",
                f"end_id: '{row['end_id']}'",
            ]
            for prop in all_props:
                value = props.get(prop)
                item_parts.append(f"`{prop}`: {self._format_value(value)}")
            items.append("{" + ", ".join(item_parts) + "}")

        items_str = ", ".join(items)
        set_clauses = [f"SET r.`{prop}` = item.`{prop}`" for prop in all_props]

        return (
            f"WITH [{items_str}] AS items "
            f"UNWIND items AS item "
            f"MATCH (source {{id: item.start_id}}) "
            f"MATCH (target {{id: item.end_id}}) "
            f"MERGE (source)-[r:{label} {{id: item.id}}]->(target) "
            + " ".join(set_clauses)
        )

    def insert_nodes(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        nodes: list[NodeData],
    ) -> int:
        """Insert nodes using COPY to staging + UNWIND MERGE."""
        if not nodes:
            return 0

        count = 0
        with conn.cursor() as cur:
            # Create staging table
            cur.execute(
                """
                CREATE TEMP TABLE _staging_nodes (
                    id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    properties JSONB NOT NULL
                ) ON COMMIT DROP
                """
            )

            # COPY all nodes to staging table
            buffer = io.StringIO()
            for node in nodes:
                props = dict(node["properties"])
                props["id"] = node["id"]
                props_json = json.dumps(props).replace("\\", "\\\\")
                buffer.write(f"{node['id']}\t{node['label']}\t{props_json}\n")

            buffer.seek(0)
            cur.copy_from(
                buffer, "_staging_nodes", columns=("id", "label", "properties")
            )

            # Get distinct labels
            cur.execute("SELECT DISTINCT label FROM _staging_nodes")
            labels = [row[0] for row in cur.fetchall()]

            # Process each label in batches
            for label in labels:
                offset = 0
                while True:
                    # Fetch batch from staging
                    cur.execute(
                        """
                        SELECT id, label, properties
                        FROM _staging_nodes
                        WHERE label = %s
                        ORDER BY id
                        LIMIT %s OFFSET %s
                        """,
                        (label, self.BATCH_SIZE, offset),
                    )
                    rows = [
                        {"id": row[0], "label": row[1], "properties": row[2]}
                        for row in cur.fetchall()
                    ]

                    if not rows:
                        break

                    # Build and execute UNWIND MERGE query
                    query = self._build_merge_nodes_query(rows, label)
                    cur.execute(
                        f"SELECT * FROM cypher('{graph_name}', $$ {query} $$) AS (n agtype)"
                    )

                    count += len(rows)
                    offset += len(rows)

            cur.execute("DROP TABLE IF EXISTS _staging_nodes")

        conn.commit()
        return count

    def insert_edges(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        edges: list[EdgeData],
    ) -> int:
        """Insert edges using COPY to staging + UNWIND MERGE."""
        if not edges:
            return 0

        count = 0
        with conn.cursor() as cur:
            # Create staging table
            cur.execute(
                """
                CREATE TEMP TABLE _staging_edges (
                    id TEXT NOT NULL,
                    label TEXT NOT NULL,
                    start_id TEXT NOT NULL,
                    end_id TEXT NOT NULL,
                    properties JSONB NOT NULL
                ) ON COMMIT DROP
                """
            )

            # COPY all edges to staging table
            buffer = io.StringIO()
            for edge in edges:
                props = dict(edge["properties"])
                props["id"] = edge["id"]
                props_json = json.dumps(props).replace("\\", "\\\\")
                buffer.write(
                    f"{edge['id']}\t{edge['label']}\t{edge['start_id']}\t{edge['end_id']}\t{props_json}\n"
                )

            buffer.seek(0)
            cur.copy_from(
                buffer,
                "_staging_edges",
                columns=("id", "label", "start_id", "end_id", "properties"),
            )

            # Get distinct labels
            cur.execute("SELECT DISTINCT label FROM _staging_edges")
            labels = [row[0] for row in cur.fetchall()]

            # Process each label in batches
            for label in labels:
                offset = 0
                while True:
                    # Fetch batch from staging
                    cur.execute(
                        """
                        SELECT id, label, start_id, end_id, properties
                        FROM _staging_edges
                        WHERE label = %s
                        ORDER BY id
                        LIMIT %s OFFSET %s
                        """,
                        (label, self.BATCH_SIZE, offset),
                    )
                    rows = [
                        {
                            "id": row[0],
                            "label": row[1],
                            "start_id": row[2],
                            "end_id": row[3],
                            "properties": row[4],
                        }
                        for row in cur.fetchall()
                    ]

                    if not rows:
                        break

                    # Build and execute UNWIND MERGE query
                    query = self._build_merge_edges_query(rows, label)
                    cur.execute(
                        f"SELECT * FROM cypher('{graph_name}', $$ {query} $$) AS (r agtype)"
                    )

                    count += len(rows)
                    offset += len(rows)

            cur.execute("DROP TABLE IF EXISTS _staging_edges")

        conn.commit()
        return count

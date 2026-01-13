"""Strategy 1: UNWIND MERGE (batched Cypher).

This strategy batches multiple entities into a single query using UNWIND.
Instead of N roundtrips, we execute 1 roundtrip with all data.

Still uses Cypher MERGE, but with dramatic reduction in overhead.
"""

from __future__ import annotations

import json

from psycopg2.extensions import connection as PsycopgConnection

from src.strategies.protocol import BulkInsertStrategy, EdgeData, NodeData


class UnwindMergeStrategy(BulkInsertStrategy):
    """Batch entities using Cypher UNWIND.

    This is the first major optimization - reducing N roundtrips to 1.
    The Cypher parser still processes all data, but network overhead
    is eliminated.

    Performance characteristics:
    - 1 database roundtrip for N entities
    - 1 Cypher parse, but larger query
    - Still bottlenecked by Cypher execution
    """

    @property
    def name(self) -> str:
        return "1. UNWIND MERGE"

    @property
    def description(self) -> str:
        return "Batched UNWIND MERGE (1 roundtrip)"

    def _escape_cypher_string(self, value: str) -> str:
        """Escape a string for use in Cypher."""
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def _format_value(self, value) -> str:
        """Format a Python value for Cypher."""
        if isinstance(value, str):
            escaped = self._escape_cypher_string(value)
            return f"'{escaped}'"
        elif isinstance(value, bool):
            return "true" if value else "false"
        elif value is None:
            return "null"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, list):
            items = [self._format_value(v) for v in value]
            return "[" + ", ".join(items) + "]"
        else:
            # For complex types, serialize as JSON string
            escaped = self._escape_cypher_string(json.dumps(value))
            return f"'{escaped}'"

    def _build_items_array(self, entities: list[dict], id_key: str = "id") -> str:
        """Build a Cypher array literal from entities."""
        items = []
        for entity in entities:
            props = []
            for key, value in entity.items():
                props.append(f"`{key}`: {self._format_value(value)}")
            items.append("{" + ", ".join(props) + "}")

        return "[" + ", ".join(items) + "]"

    def insert_nodes(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        nodes: list[NodeData],
    ) -> int:
        """Insert nodes using UNWIND MERGE."""
        if not nodes:
            return 0

        # Group nodes by label (UNWIND works best with same-label batches)
        nodes_by_label: dict[str, list[dict]] = {}
        for node in nodes:
            label = node["label"]
            if label not in nodes_by_label:
                nodes_by_label[label] = []

            # Flatten node data for Cypher
            item = {"id": node["id"], **node["properties"]}
            nodes_by_label[label].append(item)

        count = 0
        with conn.cursor() as cur:
            for label, items in nodes_by_label.items():
                items_str = self._build_items_array(items)

                # Build property SET clause dynamically
                # Get all unique keys from items
                all_keys = set()
                for item in items:
                    all_keys.update(item.keys())

                set_clauses = [f"n.`{key}` = item.`{key}`" for key in all_keys]
                set_str = ", ".join(set_clauses)

                cypher = (
                    f"UNWIND {items_str} AS item "
                    f"MERGE (n:{label} {{id: item.id}}) "
                    f"SET {set_str}"
                )

                cur.execute(
                    f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS (n agtype)"
                )
                count += len(items)

        conn.commit()
        return count

    def insert_edges(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        edges: list[EdgeData],
    ) -> int:
        """Insert edges using UNWIND MERGE."""
        if not edges:
            return 0

        # Group edges by label
        edges_by_label: dict[str, list[dict]] = {}
        for edge in edges:
            label = edge["label"]
            if label not in edges_by_label:
                edges_by_label[label] = []

            item = {
                "id": edge["id"],
                "start_id": edge["start_id"],
                "end_id": edge["end_id"],
                **edge["properties"],
            }
            edges_by_label[label].append(item)

        count = 0
        with conn.cursor() as cur:
            for label, items in edges_by_label.items():
                items_str = self._build_items_array(items)

                # Get property keys (excluding structural fields)
                prop_keys = set()
                for item in items:
                    prop_keys.update(item.keys())
                prop_keys -= {"start_id", "end_id"}  # Keep id in properties

                set_clauses = [f"r.`{key}` = item.`{key}`" for key in prop_keys]
                set_str = ", ".join(set_clauses)

                cypher = (
                    f"UNWIND {items_str} AS item "
                    f"MATCH (src {{id: item.start_id}}), (tgt {{id: item.end_id}}) "
                    f"MERGE (src)-[r:{label} {{id: item.id}}]->(tgt) "
                    f"SET {set_str}"
                )

                cur.execute(
                    f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS (r agtype)"
                )
                count += len(items)

        conn.commit()
        return count

"""Strategy 0: Individual MERGE queries (baseline).

This is the slowest approach - one Cypher MERGE query per node/edge.
Each query requires a database roundtrip and Cypher parse/plan cycle.

For N nodes, this executes N separate queries.
"""

from __future__ import annotations

import json

from psycopg2.extensions import connection as PsycopgConnection

from src.strategies.protocol import BulkInsertStrategy, EdgeData, NodeData


class IndividualMergeStrategy(BulkInsertStrategy):
    """Execute individual MERGE queries for each entity.

    This is the baseline strategy that most developers start with.
    It's simple to understand but extremely slow for bulk operations.

    Performance characteristics:
    - N database roundtrips for N entities
    - N Cypher parse/plan cycles
    - No batching benefit
    """

    @property
    def name(self) -> str:
        return "0. Individual MERGE"

    @property
    def description(self) -> str:
        return "One MERGE query per node (N roundtrips)"

    def _escape_cypher_string(self, value: str) -> str:
        """Escape a string for use in Cypher."""
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def _format_properties(self, props: dict) -> str:
        """Format properties dict as Cypher map literal."""
        if not props:
            return "{}"

        items = []
        for key, value in props.items():
            if isinstance(value, str):
                escaped = self._escape_cypher_string(value)
                items.append(f"`{key}`: '{escaped}'")
            elif isinstance(value, bool):
                items.append(f"`{key}`: {'true' if value else 'false'}")
            elif value is None:
                items.append(f"`{key}`: null")
            elif isinstance(value, (int, float)):
                items.append(f"`{key}`: {value}")
            else:
                # For complex types, serialize as JSON string
                escaped = self._escape_cypher_string(json.dumps(value))
                items.append(f"`{key}`: '{escaped}'")

        return "{" + ", ".join(items) + "}"

    def insert_nodes(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        nodes: list[NodeData],
    ) -> int:
        """Insert nodes one at a time using individual MERGE queries."""
        count = 0

        with conn.cursor() as cur:
            for node in nodes:
                # Build properties including the id
                props = dict(node["properties"])
                props["id"] = node["id"]
                props_str = self._format_properties(props)

                # Execute MERGE for this single node
                cypher = f"MERGE (n:{node['label']} {{id: '{node['id']}'}}) SET n = {props_str}"
                cur.execute(
                    f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS (n agtype)"
                )
                count += 1

        conn.commit()
        return count

    def insert_edges(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        edges: list[EdgeData],
    ) -> int:
        """Insert edges one at a time using individual MERGE queries."""
        count = 0

        with conn.cursor() as cur:
            for edge in edges:
                # Build properties including the id
                props = dict(edge["properties"])
                props["id"] = edge["id"]
                props_str = self._format_properties(props)

                # Execute MERGE for this single edge
                cypher = (
                    f"MATCH (src {{id: '{edge['start_id']}'}}), "
                    f"(tgt {{id: '{edge['end_id']}'}}) "
                    f"MERGE (src)-[r:{edge['label']} {{id: '{edge['id']}'}}]->(tgt) "
                    f"SET r = {props_str}"
                )
                cur.execute(
                    f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS (r agtype)"
                )
                count += 1

        conn.commit()
        return count

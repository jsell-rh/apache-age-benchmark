"""Strategy 3: Direct SQL INSERT (bypassing Cypher).

This is the fastest strategy - it bypasses Cypher entirely and writes
directly to AGE's internal PostgreSQL tables.

AGE stores graph data in regular PostgreSQL tables:
- Each label has its own table (e.g., "graph_name"."Person")
- Nodes have: id (graphid), properties (agtype)
- Edges have: id (graphid), start_id, end_id, properties (agtype)

By writing directly to these tables, we avoid all Cypher parsing overhead.

Data Integrity Features:
- Validates label names to prevent SQL injection
- Escapes COPY data to prevent format corruption
- Detects duplicate IDs in batch and raises error
- Detects orphaned edges (referencing non-existent nodes) and raises error
"""

from __future__ import annotations

import io
import json
import re

from psycopg2.extensions import connection as PsycopgConnection

from src.strategies.protocol import BulkInsertStrategy, EdgeData, NodeData


# Label validation: alphanumeric + underscore, must start with letter/underscore
_VALID_LABEL_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def validate_label(label: str) -> None:
    """Validate label name to prevent SQL injection."""
    if not _VALID_LABEL_RE.match(label):
        raise ValueError(
            f"Invalid label '{label}': must be alphanumeric/underscore, "
            "start with letter/underscore, max 63 chars"
        )


def escape_copy_value(value: str) -> str:
    """Escape special characters for PostgreSQL COPY format."""
    return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n")


class DirectSqlStrategy(BulkInsertStrategy):
    """Bypass Cypher entirely with direct SQL INSERT.

    This writes directly to AGE's internal PostgreSQL tables,
    achieving maximum performance for bulk operations.

    Performance characteristics:
    - No Cypher parsing or planning
    - Direct table access
    - Uses PostgreSQL COPY for data loading
    - ~100-300x faster than individual MERGE
    """

    @property
    def name(self) -> str:
        return "3. Direct SQL"

    @property
    def description(self) -> str:
        return "Direct SQL INSERT (bypass Cypher)"

    def _get_label_info(
        self, cursor, graph_name: str, label: str
    ) -> tuple[int, str] | None:
        """Get label_id and sequence name for a label.

        Returns:
            Tuple of (label_id, seq_name) if label exists, None otherwise
        """
        cursor.execute(
            """
            SELECT l.id, l.seq_name
            FROM ag_catalog.ag_label l
            JOIN ag_catalog.ag_graph g ON l.graph = g.graphid
            WHERE g.name = %s AND l.name = %s
            """,
            (graph_name, label),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return (row[0], row[1])

    def _escape_cypher_string(self, value: str) -> str:
        """Escape a string for use in Cypher."""
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def _format_cypher_properties(self, props: dict) -> str:
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
                escaped = self._escape_cypher_string(json.dumps(value))
                items.append(f"`{key}`: '{escaped}'")

        return "{" + ", ".join(items) + "}"

    def _create_label_via_cypher(
        self,
        cursor,
        graph_name: str,
        label: str,
        entity_id: str,
        properties: dict,
    ) -> None:
        """Create a label by inserting the first entity via Cypher.

        AGE automatically creates label tables when the first entity
        with that label is created via Cypher.
        """
        props_str = self._format_cypher_properties(properties)
        cypher = f"CREATE (n:{label} {props_str})"
        cursor.execute(
            f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS (n agtype)"
        )

    def _create_edge_label_via_cypher(
        self,
        cursor,
        graph_name: str,
        label: str,
        edge_id: str,
        start_id: str,
        end_id: str,
        properties: dict,
    ) -> None:
        """Create an edge label by inserting the first edge via Cypher."""
        props_str = self._format_cypher_properties(properties)
        cypher = (
            f"MATCH (src {{id: '{start_id}'}}), (tgt {{id: '{end_id}'}}) "
            f"CREATE (src)-[r:{label} {props_str}]->(tgt)"
        )
        cursor.execute(
            f"SELECT * FROM cypher('{graph_name}', $$ {cypher} $$) AS (r agtype)"
        )

    def _check_duplicates(self, cursor, table: str, entity_type: str) -> None:
        """Check for duplicate IDs in staging table."""
        cursor.execute(
            f"SELECT id, COUNT(*) FROM {table} GROUP BY id HAVING COUNT(*) > 1"
        )
        dupes = cursor.fetchall()
        if dupes:
            dupe_ids = [row[0] for row in dupes[:5]]
            raise ValueError(
                f"Duplicate {entity_type} IDs in batch: {', '.join(dupe_ids)}"
                + (f" (and {len(dupes) - 5} more)" if len(dupes) > 5 else "")
            )

    def insert_nodes(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        nodes: list[NodeData],
    ) -> int:
        """Insert nodes using direct SQL INSERT."""
        if not nodes:
            return 0

        # Validate all labels upfront
        for node in nodes:
            validate_label(node["label"])

        count = 0
        with conn.cursor() as cur:
            # Group by label
            nodes_by_label: dict[str, list[NodeData]] = {}
            for node in nodes:
                label = node["label"]
                if label not in nodes_by_label:
                    nodes_by_label[label] = []
                nodes_by_label[label].append(node)

            for label, label_nodes in nodes_by_label.items():
                # Check if label exists, create if needed
                label_info = self._get_label_info(cur, graph_name, label)
                first_node = label_nodes[0]
                skip_first = False

                if label_info is None:
                    # Create label by inserting first node via Cypher
                    props = dict(first_node["properties"])
                    props["id"] = first_node["id"]
                    self._create_label_via_cypher(
                        cur, graph_name, label, first_node["id"], props
                    )
                    label_info = self._get_label_info(cur, graph_name, label)
                    skip_first = True
                    count += 1

                if label_info is None:
                    raise ValueError(f"Failed to create label '{label}'")

                label_id, seq_name = label_info

                # Create staging table
                cur.execute(
                    """
                    CREATE TEMP TABLE _staging_nodes (
                        id TEXT NOT NULL,
                        properties JSONB NOT NULL
                    ) ON COMMIT DROP
                    """
                )

                # COPY remaining nodes to staging (with proper escaping)
                buffer = io.StringIO()
                nodes_to_insert = label_nodes[1:] if skip_first else label_nodes
                for node in nodes_to_insert:
                    props = dict(node["properties"])
                    props["id"] = node["id"]
                    escaped_id = escape_copy_value(node["id"])
                    escaped_props = escape_copy_value(json.dumps(props))
                    buffer.write(f"{escaped_id}\t{escaped_props}\n")

                if nodes_to_insert:
                    buffer.seek(0)
                    cur.copy_from(
                        buffer, "_staging_nodes", columns=("id", "properties")
                    )

                    # Check for duplicate IDs
                    self._check_duplicates(cur, "_staging_nodes", "node")

                    # Update existing nodes
                    cur.execute(
                        f"""
                        UPDATE "{graph_name}"."{label}" AS t
                        SET properties = (s.properties::text)::ag_catalog.agtype
                        FROM _staging_nodes AS s
                        WHERE ag_catalog.agtype_object_field_text_agtype(
                            t.properties, '"id"'::ag_catalog.agtype
                        ) = s.id
                        """
                    )

                    # Insert new nodes
                    cur.execute(
                        f"""
                        INSERT INTO "{graph_name}"."{label}" (id, properties)
                        SELECT
                            ag_catalog._graphid(%s, nextval('"{graph_name}"."{seq_name}"')),
                            (s.properties::text)::ag_catalog.agtype
                        FROM _staging_nodes AS s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM "{graph_name}"."{label}" AS t
                            WHERE ag_catalog.agtype_object_field_text_agtype(
                                t.properties, '"id"'::ag_catalog.agtype
                            ) = s.id
                        )
                        """,
                        (label_id,),
                    )
                    count += len(nodes_to_insert)

                cur.execute("DROP TABLE IF EXISTS _staging_nodes")

        conn.commit()
        return count

    def _check_orphaned_edges(self, cursor, graph_name: str) -> None:
        """Check for edges referencing non-existent nodes."""
        cursor.execute(
            """
            SELECT id, start_id, end_id, start_graphid, end_graphid
            FROM _staging_edges
            WHERE start_graphid IS NULL OR end_graphid IS NULL
            """
        )
        orphans = cursor.fetchall()
        if orphans:
            missing = set()
            for _, start_id, end_id, start_gid, end_gid in orphans:
                if start_gid is None:
                    missing.add(start_id)
                if end_gid is None:
                    missing.add(end_id)
            missing_list = sorted(missing)[:5]
            raise ValueError(
                f"Orphaned edges: {len(orphans)} edge(s) reference non-existent nodes. "
                f"Missing: {', '.join(missing_list)}"
                + (f" (and {len(missing) - 5} more)" if len(missing) > 5 else "")
            )

    def insert_edges(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        edges: list[EdgeData],
    ) -> int:
        """Insert edges using direct SQL INSERT."""
        if not edges:
            return 0

        # Validate all labels upfront
        for edge in edges:
            validate_label(edge["label"])

        count = 0
        with conn.cursor() as cur:
            # Group by label
            edges_by_label: dict[str, list[EdgeData]] = {}
            for edge in edges:
                label = edge["label"]
                if label not in edges_by_label:
                    edges_by_label[label] = []
                edges_by_label[label].append(edge)

            for label, label_edges in edges_by_label.items():
                # Check if label exists, create if needed
                label_info = self._get_label_info(cur, graph_name, label)
                first_edge = label_edges[0]
                skip_first = False

                if label_info is None:
                    # Create label by inserting first edge via Cypher
                    props = dict(first_edge["properties"])
                    props["id"] = first_edge["id"]
                    self._create_edge_label_via_cypher(
                        cur,
                        graph_name,
                        label,
                        first_edge["id"],
                        first_edge["start_id"],
                        first_edge["end_id"],
                        props,
                    )
                    label_info = self._get_label_info(cur, graph_name, label)
                    skip_first = True
                    count += 1

                if label_info is None:
                    raise ValueError(f"Failed to create edge label '{label}'")

                label_id, seq_name = label_info

                # Create staging table with graphid columns
                cur.execute(
                    """
                    CREATE TEMP TABLE _staging_edges (
                        id TEXT NOT NULL,
                        start_id TEXT NOT NULL,
                        end_id TEXT NOT NULL,
                        start_graphid ag_catalog.graphid,
                        end_graphid ag_catalog.graphid,
                        properties JSONB NOT NULL
                    ) ON COMMIT DROP
                    """
                )

                # COPY remaining edges to staging (with proper escaping)
                buffer = io.StringIO()
                edges_to_insert = label_edges[1:] if skip_first else label_edges
                for edge in edges_to_insert:
                    props = dict(edge["properties"])
                    props["id"] = edge["id"]
                    row = "\t".join([
                        escape_copy_value(edge["id"]),
                        escape_copy_value(edge["start_id"]),
                        escape_copy_value(edge["end_id"]),
                        escape_copy_value(json.dumps(props)),
                    ])
                    buffer.write(row + "\n")

                if edges_to_insert:
                    buffer.seek(0)
                    cur.copy_from(
                        buffer,
                        "_staging_edges",
                        columns=("id", "start_id", "end_id", "properties"),
                    )

                    # Check for duplicate IDs
                    self._check_duplicates(cur, "_staging_edges", "edge")

                    # Resolve graphids in two separate queries (avoids cartesian join)
                    cur.execute(
                        f"""
                        UPDATE _staging_edges AS s
                        SET start_graphid = v.id
                        FROM "{graph_name}"._ag_label_vertex AS v
                        WHERE ag_catalog.agtype_object_field_text_agtype(
                            v.properties, '"id"'::ag_catalog.agtype
                        ) = s.start_id
                        """
                    )
                    cur.execute(
                        f"""
                        UPDATE _staging_edges AS s
                        SET end_graphid = v.id
                        FROM "{graph_name}"._ag_label_vertex AS v
                        WHERE ag_catalog.agtype_object_field_text_agtype(
                            v.properties, '"id"'::ag_catalog.agtype
                        ) = s.end_id
                        """
                    )

                    # Check for orphaned edges (fail fast instead of silent drop)
                    self._check_orphaned_edges(cur, graph_name)

                    # Update existing edges
                    cur.execute(
                        f"""
                        UPDATE "{graph_name}"."{label}" AS t
                        SET properties = (s.properties::text)::ag_catalog.agtype
                        FROM _staging_edges AS s
                        WHERE ag_catalog.agtype_object_field_text_agtype(
                            t.properties, '"id"'::ag_catalog.agtype
                        ) = s.id
                        """
                    )

                    # Insert new edges using pre-resolved graphids
                    cur.execute(
                        f"""
                        INSERT INTO "{graph_name}"."{label}" (id, start_id, end_id, properties)
                        SELECT
                            ag_catalog._graphid(%s, nextval('"{graph_name}"."{seq_name}"')),
                            s.start_graphid,
                            s.end_graphid,
                            (s.properties::text)::ag_catalog.agtype
                        FROM _staging_edges AS s
                        WHERE NOT EXISTS (
                            SELECT 1 FROM "{graph_name}"."{label}" AS e
                            WHERE ag_catalog.agtype_object_field_text_agtype(
                                e.properties, '"id"'::ag_catalog.agtype
                            ) = s.id
                        )
                        """,
                        (label_id,),
                    )
                    count += len(edges_to_insert)

                cur.execute("DROP TABLE IF EXISTS _staging_edges")

        conn.commit()
        return count

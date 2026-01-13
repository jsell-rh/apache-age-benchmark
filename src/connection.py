"""Minimal Apache AGE connection helper."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2.extensions import connection as PsycopgConnection


# Default connection settings (can be overridden via environment)
DEFAULT_HOST = os.getenv("AGE_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("AGE_PORT", "5433"))
DEFAULT_DATABASE = os.getenv("AGE_DATABASE", "benchmark")
DEFAULT_USER = os.getenv("AGE_USER", "benchmark")
DEFAULT_PASSWORD = os.getenv("AGE_PASSWORD", "benchmark")
DEFAULT_GRAPH = os.getenv("AGE_GRAPH", "benchmark_graph")


def get_connection(
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    database: str = DEFAULT_DATABASE,
    user: str = DEFAULT_USER,
    password: str = DEFAULT_PASSWORD,
) -> PsycopgConnection:
    """Create a new database connection with AGE extension loaded.

    Returns:
        A psycopg2 connection ready for AGE operations.
    """
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
    )

    # Load AGE extension and set search path
    with conn.cursor() as cur:
        cur.execute("LOAD 'age';")
        cur.execute('SET search_path = ag_catalog, "$user", public;')
    conn.commit()

    return conn


def ensure_graph(conn: PsycopgConnection, graph_name: str = DEFAULT_GRAPH) -> None:
    """Create the graph if it doesn't exist.

    Args:
        conn: Database connection
        graph_name: Name of the graph to create
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s",
            (graph_name,),
        )
        if cur.fetchone() is None:
            cur.execute("SELECT ag_catalog.create_graph(%s)", (graph_name,))
    conn.commit()


def clean_graph(conn: PsycopgConnection, graph_name: str = DEFAULT_GRAPH) -> None:
    """Remove all nodes and edges from the graph.

    Args:
        conn: Database connection
        graph_name: Name of the graph to clean
    """
    with conn.cursor() as cur:
        # Check if graph exists first
        cur.execute(
            "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s",
            (graph_name,),
        )
        if cur.fetchone() is None:
            return

        # Delete all nodes (cascades to edges via DETACH DELETE)
        try:
            cur.execute(
                f"SELECT * FROM cypher('{graph_name}', $$ "
                f"MATCH (n) DETACH DELETE n "
                f"$$) AS (result agtype)"
            )
        except Exception:
            # Graph might be empty or have other issues
            pass
    conn.commit()


def drop_graph(conn: PsycopgConnection, graph_name: str = DEFAULT_GRAPH) -> None:
    """Drop the entire graph (for complete cleanup).

    Args:
        conn: Database connection
        graph_name: Name of the graph to drop
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s",
            (graph_name,),
        )
        if cur.fetchone() is not None:
            cur.execute("SELECT ag_catalog.drop_graph(%s, true)", (graph_name,))
    conn.commit()


def count_nodes(conn: PsycopgConnection, graph_name: str = DEFAULT_GRAPH) -> int:
    """Count total nodes in the graph.

    Args:
        conn: Database connection
        graph_name: Name of the graph

    Returns:
        Number of nodes in the graph
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM cypher('{graph_name}', $$ "
            f"MATCH (n) RETURN count(n) "
            f"$$) AS (count agtype)"
        )
        row = cur.fetchone()
        if row:
            # AGE returns agtype, parse as int
            return int(str(row[0]).strip('"'))
        return 0


def count_edges(conn: PsycopgConnection, graph_name: str = DEFAULT_GRAPH) -> int:
    """Count total edges in the graph.

    Args:
        conn: Database connection
        graph_name: Name of the graph

    Returns:
        Number of edges in the graph
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT * FROM cypher('{graph_name}', $$ "
            f"MATCH ()-[r]->() RETURN count(r) "
            f"$$) AS (count agtype)"
        )
        row = cur.fetchone()
        if row:
            return int(str(row[0]).strip('"'))
        return 0


@contextmanager
def managed_connection(
    graph_name: str = DEFAULT_GRAPH,
    **kwargs,
) -> Generator[PsycopgConnection, None, None]:
    """Context manager for database connections.

    Ensures connection is properly closed and graph exists.

    Args:
        graph_name: Name of the graph to use
        **kwargs: Additional connection parameters

    Yields:
        Database connection
    """
    conn = get_connection(**kwargs)
    try:
        ensure_graph(conn, graph_name)
        yield conn
    finally:
        conn.close()

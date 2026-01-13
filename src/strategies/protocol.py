"""Protocol definition for bulk insert strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypedDict

from psycopg2.extensions import connection as PsycopgConnection


class NodeData(TypedDict):
    """Data structure for a node to insert."""

    id: str
    label: str
    properties: dict


class EdgeData(TypedDict):
    """Data structure for an edge to insert."""

    id: str
    label: str
    start_id: str
    end_id: str
    properties: dict


class BulkInsertStrategy(ABC):
    """Abstract base class for bulk insert strategies.

    All strategies must implement this interface to ensure
    consistent benchmarking and comparison.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name of the strategy."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Brief description of the approach."""
        ...

    @abstractmethod
    def insert_nodes(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        nodes: list[NodeData],
    ) -> int:
        """Insert nodes into the graph.

        Args:
            conn: Database connection (with AGE loaded)
            graph_name: Name of the graph
            nodes: List of nodes to insert

        Returns:
            Number of nodes inserted
        """
        ...

    @abstractmethod
    def insert_edges(
        self,
        conn: PsycopgConnection,
        graph_name: str,
        edges: list[EdgeData],
    ) -> int:
        """Insert edges into the graph.

        Args:
            conn: Database connection (with AGE loaded)
            graph_name: Name of the graph
            edges: List of edges to insert

        Returns:
            Number of edges inserted
        """
        ...

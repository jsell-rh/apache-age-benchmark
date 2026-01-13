"""Test data generator for benchmarking."""

from __future__ import annotations

from src.strategies.protocol import EdgeData, NodeData


def generate_nodes(count: int, label: str = "TestNode") -> list[NodeData]:
    """Generate deterministic test nodes.

    Args:
        count: Number of nodes to generate
        label: Label for all nodes

    Returns:
        List of node data dictionaries
    """
    return [
        NodeData(
            id=f"node:{i}",
            label=label,
            properties={
                "name": f"Node {i}",
                "index": i,
                "created_at": "2024-01-01T00:00:00Z",
                "active": i % 2 == 0,
            },
        )
        for i in range(count)
    ]


def generate_edges(
    nodes: list[NodeData], label: str = "CONNECTS_TO"
) -> list[EdgeData]:
    """Generate edges connecting consecutive nodes.

    Creates a chain: node:0 -> node:1 -> node:2 -> ...

    Args:
        nodes: List of nodes to connect
        label: Label for all edges

    Returns:
        List of edge data dictionaries
    """
    if len(nodes) < 2:
        return []

    return [
        EdgeData(
            id=f"edge:{i}",
            label=label,
            start_id=nodes[i]["id"],
            end_id=nodes[i + 1]["id"],
            properties={
                "weight": i % 10,
                "created_at": "2024-01-01T00:00:00Z",
            },
        )
        for i in range(len(nodes) - 1)
    ]


def generate_test_data(
    node_count: int,
    node_label: str = "TestNode",
    edge_label: str = "CONNECTS_TO",
) -> tuple[list[NodeData], list[EdgeData]]:
    """Generate a complete test dataset.

    Args:
        node_count: Number of nodes to generate
        node_label: Label for nodes
        edge_label: Label for edges

    Returns:
        Tuple of (nodes, edges)
    """
    nodes = generate_nodes(node_count, node_label)
    edges = generate_edges(nodes, edge_label)
    return nodes, edges

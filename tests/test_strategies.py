"""Tests to verify all strategies produce correct results."""

import pytest

from src.connection import (
    clean_graph,
    count_edges,
    count_nodes,
    drop_graph,
    ensure_graph,
    get_connection,
)
from src.data_generator import generate_test_data
from src.strategies import (
    CopyUnwindStrategy,
    DirectSqlStrategy,
    IndividualMergeStrategy,
    UnwindMergeStrategy,
)

GRAPH_NAME = "test_benchmark_graph"


@pytest.fixture
def conn():
    """Get a database connection with clean graph."""
    connection = get_connection()
    drop_graph(connection, GRAPH_NAME)
    ensure_graph(connection, GRAPH_NAME)
    yield connection
    drop_graph(connection, GRAPH_NAME)
    connection.close()


class TestIndividualMergeStrategy:
    """Tests for Strategy 0: Individual MERGE."""

    def test_insert_nodes(self, conn):
        strategy = IndividualMergeStrategy()
        nodes, _ = generate_test_data(10)

        count = strategy.insert_nodes(conn, GRAPH_NAME, nodes)

        assert count == 10
        assert count_nodes(conn, GRAPH_NAME) == 10

    def test_insert_edges(self, conn):
        strategy = IndividualMergeStrategy()
        nodes, edges = generate_test_data(10)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)
        count = strategy.insert_edges(conn, GRAPH_NAME, edges)

        assert count == 9  # 10 nodes = 9 edges in chain
        assert count_edges(conn, GRAPH_NAME) == 9

    def test_idempotent(self, conn):
        """Running same batch twice should not create duplicates."""
        strategy = IndividualMergeStrategy()
        nodes, _ = generate_test_data(5)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)
        strategy.insert_nodes(conn, GRAPH_NAME, nodes)

        assert count_nodes(conn, GRAPH_NAME) == 5


class TestUnwindMergeStrategy:
    """Tests for Strategy 1: UNWIND MERGE."""

    def test_insert_nodes(self, conn):
        strategy = UnwindMergeStrategy()
        nodes, _ = generate_test_data(10)

        count = strategy.insert_nodes(conn, GRAPH_NAME, nodes)

        assert count == 10
        assert count_nodes(conn, GRAPH_NAME) == 10

    def test_insert_edges(self, conn):
        strategy = UnwindMergeStrategy()
        nodes, edges = generate_test_data(10)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)
        count = strategy.insert_edges(conn, GRAPH_NAME, edges)

        assert count == 9
        assert count_edges(conn, GRAPH_NAME) == 9

    def test_idempotent(self, conn):
        strategy = UnwindMergeStrategy()
        nodes, _ = generate_test_data(5)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)
        strategy.insert_nodes(conn, GRAPH_NAME, nodes)

        assert count_nodes(conn, GRAPH_NAME) == 5


class TestCopyUnwindStrategy:
    """Tests for Strategy 2: COPY + UNWIND."""

    def test_insert_nodes(self, conn):
        strategy = CopyUnwindStrategy()
        nodes, _ = generate_test_data(10)

        count = strategy.insert_nodes(conn, GRAPH_NAME, nodes)

        assert count == 10
        assert count_nodes(conn, GRAPH_NAME) == 10

    def test_insert_edges(self, conn):
        strategy = CopyUnwindStrategy()
        nodes, edges = generate_test_data(10)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)
        count = strategy.insert_edges(conn, GRAPH_NAME, edges)

        assert count == 9
        assert count_edges(conn, GRAPH_NAME) == 9

    def test_idempotent(self, conn):
        strategy = CopyUnwindStrategy()
        nodes, _ = generate_test_data(5)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)
        strategy.insert_nodes(conn, GRAPH_NAME, nodes)

        assert count_nodes(conn, GRAPH_NAME) == 5


class TestDirectSqlStrategy:
    """Tests for Strategy 3: Direct SQL."""

    def test_insert_nodes(self, conn):
        strategy = DirectSqlStrategy()
        nodes, _ = generate_test_data(10)

        count = strategy.insert_nodes(conn, GRAPH_NAME, nodes)

        assert count == 10
        assert count_nodes(conn, GRAPH_NAME) == 10

    def test_insert_edges(self, conn):
        strategy = DirectSqlStrategy()
        nodes, edges = generate_test_data(10)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)
        count = strategy.insert_edges(conn, GRAPH_NAME, edges)

        assert count == 9
        assert count_edges(conn, GRAPH_NAME) == 9

    def test_idempotent(self, conn):
        strategy = DirectSqlStrategy()
        nodes, _ = generate_test_data(5)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)
        strategy.insert_nodes(conn, GRAPH_NAME, nodes)

        assert count_nodes(conn, GRAPH_NAME) == 5


class TestAllStrategiesProduceSameResults:
    """Verify all strategies produce identical graph state."""

    @pytest.mark.parametrize(
        "strategy_class",
        [
            IndividualMergeStrategy,
            UnwindMergeStrategy,
            CopyUnwindStrategy,
            DirectSqlStrategy,
        ],
    )
    def test_same_node_count(self, conn, strategy_class):
        """All strategies should produce the same number of nodes."""
        strategy = strategy_class()
        nodes, _ = generate_test_data(20)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)

        assert count_nodes(conn, GRAPH_NAME) == 20

    @pytest.mark.parametrize(
        "strategy_class",
        [
            IndividualMergeStrategy,
            UnwindMergeStrategy,
            CopyUnwindStrategy,
            DirectSqlStrategy,
        ],
    )
    def test_same_edge_count(self, conn, strategy_class):
        """All strategies should produce the same number of edges."""
        strategy = strategy_class()
        nodes, edges = generate_test_data(20)

        strategy.insert_nodes(conn, GRAPH_NAME, nodes)
        strategy.insert_edges(conn, GRAPH_NAME, edges)

        assert count_nodes(conn, GRAPH_NAME) == 20
        assert count_edges(conn, GRAPH_NAME) == 19

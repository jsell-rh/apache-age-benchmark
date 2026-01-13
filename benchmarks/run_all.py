#!/usr/bin/env python3
"""Benchmark runner for AGE bulk insert strategies.

Usage:
    uv run python benchmarks/run_all.py
    uv run python benchmarks/run_all.py --sizes 100,500,1000
    uv run python benchmarks/run_all.py --strategies 0,3
    uv run python benchmarks/run_all.py --timeout 600
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table

from src.connection import (
    DEFAULT_GRAPH,
    drop_graph,
    ensure_graph,
    get_connection,
    count_nodes,
    count_edges,
)
from src.data_generator import generate_test_data
from src.strategies import (
    IndividualMergeStrategy,
    UnwindMergeStrategy,
    CopyUnwindStrategy,
    DirectSqlStrategy,
)

if TYPE_CHECKING:
    from src.strategies.protocol import BulkInsertStrategy

console = Console()

# All available strategies in order
ALL_STRATEGIES: list[type[BulkInsertStrategy]] = [
    IndividualMergeStrategy,
    UnwindMergeStrategy,
    CopyUnwindStrategy,
    DirectSqlStrategy,
]

DEFAULT_SIZES = [100, 500, 1000, 5000, 10000]


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    strategy_name: str
    node_count: int
    edge_count: int
    duration_seconds: float
    nodes_per_second: float
    success: bool
    error: str | None = None


class TimeoutError(Exception):
    """Raised when a benchmark times out."""

    pass


def timeout_handler(signum, frame):
    raise TimeoutError("Benchmark timed out")


def run_benchmark(
    strategy: BulkInsertStrategy,
    node_count: int,
    graph_name: str = DEFAULT_GRAPH,
    timeout_seconds: int = 300,
) -> BenchmarkResult:
    """Run a single benchmark.

    Args:
        strategy: The strategy to benchmark
        node_count: Number of nodes to insert
        graph_name: Name of the graph
        timeout_seconds: Maximum time for the benchmark

    Returns:
        BenchmarkResult with timing information
    """
    # Generate test data
    nodes, edges = generate_test_data(node_count)

    # Get fresh connection and clean graph
    conn = get_connection()
    try:
        drop_graph(conn, graph_name)
        ensure_graph(conn, graph_name)

        # Set up timeout (Unix only)
        if hasattr(signal, "SIGALRM"):
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout_seconds)

        try:
            # Time the insertion
            start_time = time.perf_counter()

            # Insert nodes
            strategy.insert_nodes(conn, graph_name, nodes)

            # Insert edges
            strategy.insert_edges(conn, graph_name, edges)

            end_time = time.perf_counter()
            duration = end_time - start_time

            # Cancel timeout
            if hasattr(signal, "SIGALRM"):
                signal.alarm(0)

            # Verify counts
            actual_nodes = count_nodes(conn, graph_name)
            actual_edges = count_edges(conn, graph_name)

            if actual_nodes != len(nodes):
                return BenchmarkResult(
                    strategy_name=strategy.name,
                    node_count=node_count,
                    edge_count=len(edges),
                    duration_seconds=duration,
                    nodes_per_second=node_count / duration if duration > 0 else 0,
                    success=False,
                    error=f"Node count mismatch: expected {len(nodes)}, got {actual_nodes}",
                )

            return BenchmarkResult(
                strategy_name=strategy.name,
                node_count=node_count,
                edge_count=len(edges),
                duration_seconds=duration,
                nodes_per_second=node_count / duration if duration > 0 else 0,
                success=True,
            )

        except TimeoutError:
            if hasattr(signal, "SIGALRM"):
                signal.alarm(0)
            return BenchmarkResult(
                strategy_name=strategy.name,
                node_count=node_count,
                edge_count=len(edges),
                duration_seconds=timeout_seconds,
                nodes_per_second=0,
                success=False,
                error=f"Timeout after {timeout_seconds}s",
            )

        except Exception as e:
            if hasattr(signal, "SIGALRM"):
                signal.alarm(0)
            return BenchmarkResult(
                strategy_name=strategy.name,
                node_count=node_count,
                edge_count=len(edges),
                duration_seconds=0,
                nodes_per_second=0,
                success=False,
                error=str(e),
            )

    finally:
        conn.close()


def format_duration(seconds: float) -> str:
    """Format duration for display."""
    if seconds < 0.01:
        return f"{seconds * 1000:.1f}ms"
    elif seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"


def run_benchmarks(
    sizes: list[int],
    strategy_indices: list[int] | None = None,
    timeout: int = 300,
    warmup: bool = True,
) -> list[BenchmarkResult]:
    """Run all benchmarks.

    Args:
        sizes: List of node counts to test
        strategy_indices: Which strategies to run (0-3), or None for all
        timeout: Timeout per benchmark in seconds
        warmup: Run a warmup iteration first

    Returns:
        List of all benchmark results
    """
    strategies = [ALL_STRATEGIES[i]() for i in (strategy_indices or range(4))]
    results: list[BenchmarkResult] = []

    # Warmup run (smallest size, fastest strategy)
    if warmup and sizes:
        console.print("[dim]Running warmup...[/dim]")
        fastest = DirectSqlStrategy()
        run_benchmark(fastest, min(sizes), timeout_seconds=60)
        console.print("[dim]Warmup complete.[/dim]\n")

    total_runs = len(sizes) * len(strategies)
    current_run = 0

    for size in sizes:
        for strategy in strategies:
            current_run += 1

            console.print(
                f"[{current_run}/{total_runs}] Running {strategy.name} "
                f"with {size} nodes..."
            )

            result = run_benchmark(strategy, size, timeout_seconds=timeout)
            results.append(result)

            if result.success:
                console.print(
                    f"  [green]Done in {format_duration(result.duration_seconds)}[/green] "
                    f"({result.nodes_per_second:.0f} nodes/sec)"
                )
            else:
                console.print(f"  [red]Failed: {result.error}[/red]")

    return results


def display_results(results: list[BenchmarkResult], sizes: list[int]) -> None:
    """Display results in a rich table."""
    # Group results by strategy
    by_strategy: dict[str, dict[int, BenchmarkResult]] = {}
    for result in results:
        if result.strategy_name not in by_strategy:
            by_strategy[result.strategy_name] = {}
        by_strategy[result.strategy_name][result.node_count] = result

    # Create table
    table = Table(title="AGE Bulk Insert Benchmark Results")
    table.add_column("Strategy", style="cyan")

    for size in sizes:
        table.add_column(f"{size} nodes", justify="right")

    table.add_column("Speedup", justify="right", style="green")

    # Find the largest size where baseline (strategy 0) succeeded
    baseline_strategy = "0. Individual MERGE"
    largest_baseline_size: int | None = None
    for size in reversed(sizes):
        if baseline_strategy in by_strategy:
            result = by_strategy[baseline_strategy].get(size)
            if result and result.success:
                largest_baseline_size = size
                break

    # Add rows
    for strategy_name in sorted(by_strategy.keys()):
        row = [strategy_name]

        for size in sizes:
            result = by_strategy[strategy_name].get(size)
            if result:
                if result.success:
                    row.append(format_duration(result.duration_seconds))
                else:
                    if "Timeout" in (result.error or ""):
                        row.append("[red]timeout[/red]")
                    elif "Skipped" in (result.error or ""):
                        row.append("[dim]skipped[/dim]")
                    else:
                        row.append("[red]error[/red]")
            else:
                row.append("-")

        # Calculate speedup at the largest size where baseline succeeded
        speedup_str = "-"
        if largest_baseline_size is not None:
            baseline_result = by_strategy.get(baseline_strategy, {}).get(largest_baseline_size)
            current_result = by_strategy[strategy_name].get(largest_baseline_size)
            if (
                baseline_result
                and baseline_result.success
                and current_result
                and current_result.success
            ):
                speedup = baseline_result.duration_seconds / current_result.duration_seconds
                # Color red if slower than baseline, green if faster
                if speedup < 1.0:
                    speedup_str = f"[red]{speedup:.1f}x[/red]"
                elif speedup > 1.0:
                    speedup_str = f"[green]{speedup:.1f}x[/green]"
                else:
                    speedup_str = f"{speedup:.1f}x"

        row.append(speedup_str)
        table.add_row(*row)

    console.print()
    console.print(table)

    # Add explanation of speedup calculation
    if largest_baseline_size is not None:
        console.print(
            f"\n[dim]Note: Speedup calculated at {largest_baseline_size:,} nodes "
            f"(largest size where baseline succeeded)[/dim]"
        )
    else:
        console.print("\n[dim]Note: Speedup unavailable (baseline did not succeed)[/dim]")


def save_results(results: list[BenchmarkResult], output_dir: Path) -> Path:
    """Save results to JSON file."""
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"benchmark_{timestamp}.json"

    data = {
        "timestamp": datetime.now().isoformat(),
        "results": [
            {
                "strategy": r.strategy_name,
                "node_count": r.node_count,
                "edge_count": r.edge_count,
                "duration_seconds": r.duration_seconds,
                "nodes_per_second": r.nodes_per_second,
                "success": r.success,
                "error": r.error,
            }
            for r in results
        ],
    }

    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    return output_file


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark AGE bulk insert strategies"
    )
    parser.add_argument(
        "--sizes",
        type=str,
        default=",".join(map(str, DEFAULT_SIZES)),
        help=f"Comma-separated list of node counts (default: {','.join(map(str, DEFAULT_SIZES))})",
    )
    parser.add_argument(
        "--strategies",
        type=str,
        default=None,
        help="Comma-separated list of strategy indices 0-3 (default: all)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout per benchmark in seconds (default: 300)",
    )
    parser.add_argument(
        "--no-warmup",
        action="store_true",
        help="Skip warmup run",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmarks/results"),
        help="Directory for JSON output (default: benchmarks/results)",
    )

    args = parser.parse_args()

    # Parse sizes
    sizes = [int(s.strip()) for s in args.sizes.split(",")]

    # Parse strategies
    strategy_indices = None
    if args.strategies:
        strategy_indices = [int(s.strip()) for s in args.strategies.split(",")]

    console.print("[bold]AGE Bulk Insert Benchmark[/bold]")
    console.print(f"Sizes: {sizes}")
    console.print(
        f"Strategies: {strategy_indices if strategy_indices else 'all (0-3)'}"
    )
    console.print(f"Timeout: {args.timeout}s per benchmark")
    console.print()

    # Check database connection
    try:
        conn = get_connection()
        conn.close()
        console.print("[green]Database connection OK[/green]\n")
    except Exception as e:
        console.print(f"[red]Database connection failed: {e}[/red]")
        console.print("Make sure PostgreSQL with AGE is running:")
        console.print("  docker compose up -d")
        sys.exit(1)

    # Run benchmarks
    results = run_benchmarks(
        sizes=sizes,
        strategy_indices=strategy_indices,
        timeout=args.timeout,
        warmup=not args.no_warmup,
    )

    # Display results
    display_results(results, sizes)

    # Save results
    output_file = save_results(results, args.output_dir)
    console.print(f"\nResults saved to: {output_file}")


if __name__ == "__main__":
    main()

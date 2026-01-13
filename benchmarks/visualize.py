#!/usr/bin/env python3
"""Visualize benchmark results from JSON files.

Usage:
    uv run python -m benchmarks.visualize benchmarks/results/*.json
    uv run python -m benchmarks.visualize results.json --output chart.png
    uv run python -m benchmarks.visualize results.json --type bar
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def load_results(file_paths: list[Path]) -> list[dict]:
    """Load benchmark results from JSON files."""
    all_results = []
    for path in file_paths:
        with open(path) as f:
            data = json.load(f)
            all_results.extend(data.get("results", []))
    return all_results


def plot_duration_comparison(
    results: list[dict],
    output_path: Path | None = None,
    title: str = "AGE Bulk Insert: Duration by Strategy",
) -> None:
    """Create a grouped bar chart comparing durations across strategies and sizes."""
    # Group results by strategy and node_count
    strategies = sorted(set(r["strategy"] for r in results))
    sizes = sorted(set(r["node_count"] for r in results))

    # Create data matrix
    data = {}
    for strategy in strategies:
        data[strategy] = []
        for size in sizes:
            matching = [
                r
                for r in results
                if r["strategy"] == strategy and r["node_count"] == size
            ]
            if matching and matching[0]["success"]:
                data[strategy].append(matching[0]["duration_seconds"])
            else:
                data[strategy].append(None)

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 6))

    x = np.arange(len(sizes))
    width = 0.2
    multiplier = 0

    colors = ["#e74c3c", "#f39c12", "#3498db", "#2ecc71"]

    for i, (strategy, durations) in enumerate(data.items()):
        offset = width * multiplier
        # Replace None with 0 for plotting, but we'll handle labels
        plot_durations = [d if d is not None else 0 for d in durations]
        bars = ax.bar(
            x + offset,
            plot_durations,
            width,
            label=strategy,
            color=colors[i % len(colors)],
        )

        # Add value labels on bars
        for bar, duration in zip(bars, durations):
            if duration is not None:
                height = bar.get_height()
                if duration < 1:
                    label = f"{duration*1000:.0f}ms"
                elif duration < 60:
                    label = f"{duration:.1f}s"
                else:
                    label = f"{duration/60:.1f}m"
                ax.annotate(
                    label,
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                    rotation=45,
                )
            else:
                # Mark as timeout/error
                ax.annotate(
                    "N/A",
                    xy=(bar.get_x() + bar.get_width() / 2, 0.1),
                    ha="center",
                    va="bottom",
                    fontsize=8,
                    color="gray",
                )

        multiplier += 1

    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Duration (seconds)")
    ax.set_title(title)
    ax.set_xticks(x + width * (len(strategies) - 1) / 2)
    ax.set_xticklabels([f"{s:,}" for s in sizes])
    ax.legend(loc="upper left")
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Chart saved to: {output_path}")
    else:
        plt.show()


def plot_throughput_comparison(
    results: list[dict],
    output_path: Path | None = None,
    title: str = "AGE Bulk Insert: Throughput by Strategy",
) -> None:
    """Create a line chart comparing throughput (nodes/sec) across sizes."""
    strategies = sorted(set(r["strategy"] for r in results))
    sizes = sorted(set(r["node_count"] for r in results))

    fig, ax = plt.subplots(figsize=(12, 6))

    colors = ["#e74c3c", "#f39c12", "#3498db", "#2ecc71"]
    markers = ["o", "s", "^", "D"]

    for i, strategy in enumerate(strategies):
        throughputs = []
        valid_sizes = []
        for size in sizes:
            matching = [
                r
                for r in results
                if r["strategy"] == strategy and r["node_count"] == size
            ]
            if matching and matching[0]["success"]:
                throughputs.append(matching[0]["nodes_per_second"])
                valid_sizes.append(size)

        if throughputs:
            ax.plot(
                valid_sizes,
                throughputs,
                marker=markers[i % len(markers)],
                label=strategy,
                color=colors[i % len(colors)],
                linewidth=2,
                markersize=8,
            )

    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Throughput (nodes/second)")
    ax.set_title(title)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Chart saved to: {output_path}")
    else:
        plt.show()


def plot_speedup_chart(
    results: list[dict],
    output_path: Path | None = None,
    title: str = "AGE Bulk Insert: Speedup vs Baseline",
) -> None:
    """Create a bar chart showing speedup of each strategy vs baseline."""
    strategies = sorted(set(r["strategy"] for r in results))
    sizes = sorted(set(r["node_count"] for r in results))

    # Find baseline strategy (0. Individual MERGE)
    baseline_strategy = next((s for s in strategies if "Individual" in s), strategies[0])

    fig, ax = plt.subplots(figsize=(12, 6))

    x = np.arange(len(sizes))
    width = 0.2
    multiplier = 0

    colors = ["#e74c3c", "#f39c12", "#3498db", "#2ecc71"]

    for i, strategy in enumerate(strategies):
        speedups = []
        for size in sizes:
            baseline_result = [
                r
                for r in results
                if r["strategy"] == baseline_strategy and r["node_count"] == size
            ]
            current_result = [
                r
                for r in results
                if r["strategy"] == strategy and r["node_count"] == size
            ]

            if (
                baseline_result
                and current_result
                and baseline_result[0]["success"]
                and current_result[0]["success"]
            ):
                baseline_time = baseline_result[0]["duration_seconds"]
                current_time = current_result[0]["duration_seconds"]
                speedup = baseline_time / current_time if current_time > 0 else 0
                speedups.append(speedup)
            else:
                speedups.append(0)

        offset = width * multiplier
        bars = ax.bar(
            x + offset,
            speedups,
            width,
            label=strategy,
            color=colors[i % len(colors)],
        )

        # Add value labels
        for bar, speedup in zip(bars, speedups):
            if speedup > 0:
                height = bar.get_height()
                ax.annotate(
                    f"{speedup:.1f}x",
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )

        multiplier += 1

    ax.set_xlabel("Number of Nodes")
    ax.set_ylabel("Speedup (vs Individual MERGE)")
    ax.set_title(title)
    ax.set_xticks(x + width * (len(strategies) - 1) / 2)
    ax.set_xticklabels([f"{s:,}" for s in sizes])
    ax.legend(loc="upper left")
    ax.axhline(y=1, color="gray", linestyle="--", alpha=0.5)
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()

    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Chart saved to: {output_path}")
    else:
        plt.show()


def create_all_charts(
    results: list[dict],
    output_dir: Path,
) -> None:
    """Create all chart types and save to output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)

    plot_duration_comparison(results, output_dir / "duration_comparison.png")
    plot_throughput_comparison(results, output_dir / "throughput_comparison.png")
    plot_speedup_chart(results, output_dir / "speedup_comparison.png")

    print(f"\nAll charts saved to: {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Visualize benchmark results from JSON files"
    )
    parser.add_argument(
        "files",
        nargs="+",
        type=Path,
        help="JSON result files to visualize",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output file path (PNG). If not specified, displays interactively.",
    )
    parser.add_argument(
        "--type",
        "-t",
        choices=["duration", "throughput", "speedup", "all"],
        default="all",
        help="Type of chart to generate (default: all)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmarks/charts"),
        help="Output directory for 'all' chart type (default: benchmarks/charts)",
    )

    args = parser.parse_args()

    # Load results
    results = load_results(args.files)

    if not results:
        print("No results found in input files")
        return

    print(f"Loaded {len(results)} benchmark results")

    # Filter to successful results only for summary
    successful = [r for r in results if r["success"]]
    print(f"  - {len(successful)} successful runs")
    print(f"  - Strategies: {sorted(set(r['strategy'] for r in results))}")
    print(f"  - Sizes: {sorted(set(r['node_count'] for r in results))}")

    if args.type == "all":
        create_all_charts(results, args.output_dir)
    elif args.type == "duration":
        plot_duration_comparison(results, args.output)
    elif args.type == "throughput":
        plot_throughput_comparison(results, args.output)
    elif args.type == "speedup":
        plot_speedup_chart(results, args.output)


if __name__ == "__main__":
    main()

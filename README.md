# Apache AGE Bulk Insert: From 2 Hours to 27 Seconds

A benchmarking sandbox demonstrating how to achieve **~270x faster** bulk loading in Apache AGE by understanding its internals and progressively optimizing the approach.

## The Problem

When building a knowledge graph application with [Apache AGE](https://age.apache.org/) (a PostgreSQL extension for graph databases), we needed to ingest ~11,000 nodes with their relationships. Using the "obvious" approach of individual Cypher MERGE queries, the initial load took **over 2 hours**.

After a series of optimizations, we got it down to **27 seconds** - a ~270x improvement.

This repository contains minimal, reproducible benchmarks showing each step of that journey.

## Quick Start

```bash
# Clone and enter the directory
cd age-bulk-insert-sandbox

# Start PostgreSQL with AGE
docker compose up -d

# Wait for database to be ready
sleep 5

# Install dependencies
uv sync

# Run benchmarks
uv run python -m benchmarks.run_all --sizes 100,500,1000

# Visualize results
uv run python -m benchmarks.visualize benchmarks/results/benchmark_*.json
```

## Benchmark Results

Here are actual results from running the benchmarks:

```
                                       AGE Bulk Insert Benchmark Results                                       
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┓
┃ Strategy            ┃ 100 nodes ┃ 500 nodes ┃ 1000 nodes ┃ 5000 nodes ┃ 10000 nodes ┃ 50000 nodes ┃ Speedup ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━┩
│ 0. Individual MERGE │      54ms │     336ms │      943ms │     15.65s │      57.43s │     22m 45s │    1.0x │
│ 1. UNWIND MERGE     │      27ms │     362ms │      1.26s │     31.07s │      2m 19s │     58m 52s │    2.0x │
│ 2. COPY + UNWIND    │      42ms │     893ms │      3.75s │     1m 34s │      6m 34s │     timeout │    1.3x │
│ 3. Direct SQL       │      14ms │      24ms │       32ms │      138ms │       401ms │       1.11s │    3.9x │
└─────────────────────┴───────────┴───────────┴────────────┴────────────┴─────────────┴─────────────┴─────────┘
```

**Note:** The speedup column compares each strategy against the baseline (Strategy 0: Individual MERGE) at the largest dataset size where the baseline successfully completed. For example, if the baseline succeeded at 50,000 nodes, speedup is calculated as: `baseline_time(50k) / strategy_time(50k)`. Values are color-coded: <span style="color:green">green for faster</span>, <span style="color:red">red for slower</span> than baseline.

**Key insight**: At scale, the Direct SQL approach achieves **~143x speedup** over individual MERGE queries.

## The Strategies

### Strategy 0: Individual MERGE (Baseline)

The most intuitive approach - execute one Cypher MERGE per node:

```python
for node in nodes:
    props = {id: node['id'], name: node['name'], ...}
    cypher = f"MERGE (n:Person {{id: '{node['id']}'}}) SET n = {props}"
    cursor.execute(f"SELECT * FROM cypher('graph', $$ {cypher} $$) AS (n agtype)")
```

**Why it's slow:**
- N database roundtrips for N nodes
- N Cypher parse/plan cycles
- No batching benefit

**Performance:** ~1,000-1,800 nodes/second (varies with node count)

### Strategy 1: UNWIND MERGE

Batch all nodes into a single query using Cypher's UNWIND:

```python
# Build items array in Cypher syntax
items_str = "[{id: 'node:1', name: 'Alice'}, {id: 'node:2', name: 'Bob'}]"
cypher = f"""
UNWIND {items_str} AS item
MERGE (n:Person {{id: item.id}})
SET n.id = item.id, n.name = item.name
"""
cursor.execute(f"SELECT * FROM cypher('graph', $$ {cypher} $$) AS (n agtype)")
```

**Why it helps:**
- 1 database roundtrip instead of N
- Single Cypher parse/plan (though larger query)

**Performance:** ~160-800 nodes/second (degrades at scale due to large query parsing)

### Strategy 2: COPY + UNWIND MERGE

Use PostgreSQL's COPY protocol to rapidly load data to a staging table, then UNWIND MERGE in batches:

```python
# 1. Create staging table
cursor.execute("""
    CREATE TEMP TABLE staging (
        id TEXT NOT NULL,
        label TEXT NOT NULL,
        properties JSONB NOT NULL
    ) ON COMMIT DROP
""")

# 2. COPY is extremely fast (binary protocol)
cursor.copy_from(buffer, "staging", columns=["id", "label", "properties"])

# 3. Fetch batches from staging and UNWIND MERGE
for batch in fetch_batches(staging):
    cypher = build_unwind_merge_query(batch)
    cursor.execute(f"SELECT * FROM cypher('graph', $$ {cypher} $$) AS (n agtype)")
```

**Why it's slower than expected:**
- COPY is fast, but we still pay Cypher overhead per batch
- The batch fetching from staging adds overhead
- At scale, the Cypher MERGE becomes the bottleneck

**Performance:** ~25-500 nodes/second (bottlenecked by Cypher)

### Strategy 3: Direct SQL INSERT (The Breakthrough)

The key insight: **Apache AGE stores graph data in regular PostgreSQL tables**.

Each label has its own table in the graph's schema:
- `"graph_name"."Person"` for Person nodes
- `"graph_name"."KNOWS"` for KNOWS edges

By writing directly to these tables, we bypass Cypher entirely:

```python
# 1. Create staging table and COPY data (fast)
cursor.execute("""
    CREATE TEMP TABLE staging (
        id TEXT NOT NULL,
        properties JSONB NOT NULL
    ) ON COMMIT DROP
""")
cursor.copy_from(buffer, "staging", columns=["id", "properties"])

# 2. Get label metadata
cursor.execute("""
    SELECT l.id, l.seq_name
    FROM ag_catalog.ag_label l
    JOIN ag_catalog.ag_graph g ON l.graph = g.graphid
    WHERE g.name = 'graph' AND l.name = 'Person'
""")
label_id, seq_name = cursor.fetchone()

# 3. Direct INSERT (bypass Cypher!)
cursor.execute(f"""
    INSERT INTO "graph"."Person" (id, properties)
    SELECT
        ag_catalog._graphid({label_id}, nextval('"graph"."{seq_name}"')),
        (s.properties::text)::ag_catalog.agtype
    FROM staging s
    WHERE NOT EXISTS (
        SELECT 1 FROM "graph"."Person" t
        WHERE ag_catalog.agtype_object_field_text_agtype(
            t.properties, '"id"'::ag_catalog.agtype
        ) = s.id
    )
""")
```

**Why it's so much faster:**
- No Cypher parsing or planning
- Direct PostgreSQL table operations
- Single INSERT for all nodes of a label
- Can use all PostgreSQL optimizations

**Performance:** ~25,000-36,000 nodes/second

## Understanding AGE Internals

Apache AGE is a PostgreSQL extension that adds graph capabilities. Internally, it uses:

### Graph Metadata
```sql
-- List all graphs
SELECT * FROM ag_catalog.ag_graph;

-- List all labels in a graph
SELECT * FROM ag_catalog.ag_label WHERE graph = (
    SELECT graphid FROM ag_catalog.ag_graph WHERE name = 'my_graph'
);
```

### Node Storage
Each vertex label creates a table inheriting from `_ag_label_vertex`:
```sql
-- Parent table for all vertices
SELECT * FROM "my_graph"._ag_label_vertex;

-- Specific label table
SELECT * FROM "my_graph"."Person";
```

Columns:
- `id` - graphid (computed via `ag_catalog._graphid(label_id, sequence_value)`)
- `properties` - agtype (JSON-like properties including the logical `id`)

### Edge Storage
Each edge label creates a table inheriting from `_ag_label_edge`:
```sql
SELECT * FROM "my_graph"."KNOWS";
```

Columns:
- `id` - graphid
- `start_id` - graphid of source vertex
- `end_id` - graphid of target vertex
- `properties` - agtype

### Key Functions
- `ag_catalog._graphid(label_id, entry_id)` - Compute a graphid
- `ag_catalog.agtype_object_field_text_agtype(props, key)` - Extract text property
- `nextval('"schema"."sequence"')` - Get next sequence value (note the quoted identifiers)

## CLI Options

```bash
# Run with specific sizes
uv run python -m benchmarks.run_all --sizes 100,500,1000,5000

# Run specific strategies only (0-3)
uv run python -m benchmarks.run_all --strategies 0,3

# Custom timeout
uv run python -m benchmarks.run_all --timeout 600

# All options
uv run python -m benchmarks.run_all \
    --sizes 100,1000,10000 \
    --strategies 1,2,3 \
    --timeout 300 \
    --no-warmup

# Visualize benchmark results
uv run python -m benchmarks.visualize benchmarks/results/*.json --output chart.png
```

## When to Use Each Strategy

| Use Case | Recommended Strategy |
|----------|---------------------|
| Interactive CRUD (1-10 entities) | 0. Individual MERGE |
| Small batch updates (10-100 entities) | 1. UNWIND MERGE |
| Bulk loading (1000+ entities) | 3. Direct SQL |

Note: Strategy 2 (COPY + UNWIND) showed disappointing results in our benchmarks - it combines the overhead of staging tables with Cypher's parsing overhead, making it slower than simpler approaches.

## Caveats for Direct SQL

The Direct SQL strategy bypasses Cypher, which means:

1. **Fragility**: You're depending on AGE's internal table structure, which could change between versions
2. **Label creation**: AGE creates label tables lazily - you need to create the first entity via Cypher to initialize the table
3. **Manual ID management**: You must correctly compute graphids using `_graphid()`
4. **Edge graphid resolution**: For edges, you must look up the graphid of source/target nodes

### Data Integrity Protections

This implementation includes safeguards to catch common errors:

- **Label validation**: Rejects invalid label names (prevents SQL injection)
- **Duplicate detection**: Fails fast if batch contains duplicate IDs
- **Orphaned edge detection**: Fails fast if edges reference non-existent nodes (instead of silently dropping them)
- **COPY escaping**: Properly escapes tabs/newlines in data to prevent format corruption

Use Direct SQL when:
- Performance is critical
- You're doing one-time bulk loads
- You control the AGE version

Stick with Cypher when:
- You need the abstraction layer
- Cross-version compatibility matters
- You're doing interactive operations

## Project Structure

```
age-bulk-insert-sandbox/
├── compose.yaml              # Docker Compose for Apache AGE
├── pyproject.toml            # Dependencies (uv)
├── README.md                 # This file
├── src/
│   ├── connection.py         # AGE connection helper
│   ├── data_generator.py     # Test data generation
│   └── strategies/
│       ├── protocol.py       # Strategy interface (ABC)
│       ├── s0_individual_merge.py
│       ├── s1_unwind_merge.py
│       ├── s2_copy_unwind.py
│       └── s3_direct_sql.py
├── benchmarks/
│   ├── run_all.py            # Benchmark runner with CLI
│   ├── visualize.py          # Chart generation from results
│   ├── results/              # JSON output from benchmarks
│   └── charts/               # Generated charts (PNG)
└── tests/
    └── test_strategies.py    # Correctness tests
```

## References

- [Apache AGE Documentation](https://age.apache.org/age-manual/master/index.html)
- [AGE GitHub Repository](https://github.com/apache/age)
- [Microsoft Azure AGE Performance Guide](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/generative-ai-age-performance)
- [PostgreSQL COPY Protocol](https://www.postgresql.org/docs/current/sql-copy.html)

## License

MIT

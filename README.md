# Apache AGE Bulk Insert: From 2 Hours to 27 Seconds

## The Problem

We needed to load ~11,000 nodes into [Apache AGE](https://age.apache.org/) (a PostgreSQL graph extension). Using the obvious approach—individual Cypher MERGE queries—took **over 2 hours**.

That's unacceptable. This repo documents how we got it down to **27 seconds**.

## TL;DR

| Approach | 10k nodes | Why |
|----------|-----------|-----|
| Individual MERGE | 57s | N roundtrips, N query parses |
| UNWIND batch | 2m 19s | One roundtrip, but Cypher chokes on large queries |
| COPY + UNWIND | 6m 34s | Staging table overhead + Cypher overhead |
| **Direct SQL** | **0.4s** | Bypass Cypher entirely, write to AGE's tables |

The trick: AGE stores graphs in regular PostgreSQL tables. Skip Cypher, write directly.

---

## The Journey

### Attempt 1: Individual MERGE (Baseline)

The textbook approach—one query per node:

```python
for node in nodes:
    cursor.execute(f"""
        SELECT * FROM cypher('graph', $$ 
            MERGE (n:Person {{id: '{node.id}'}}) SET n = {props}
        $$) AS (n agtype)
    """)
```

**Result:** ~1,500 nodes/second. For 11k nodes: **~7 seconds**. Not terrible for small loads.

**But it doesn't scale.** At 50k nodes: **22 minutes**.

### Attempt 2: UNWIND Batching

Surely batching helps? Send all nodes in one query:

```python
items = "[{id: '1', name: 'Alice'}, {id: '2', name: 'Bob'}, ...]"
cursor.execute(f"""
    SELECT * FROM cypher('graph', $$ 
        UNWIND {items} AS item
        MERGE (n:Person {{id: item.id}}) SET n = item
    $$) AS (n agtype)
""")
```

**Result:** Faster for small batches, but **slower at scale**. The Cypher parser chokes on giant query strings. At 50k nodes: **59 minutes**. *Worse than individual queries.*

### Attempt 3: COPY to Staging Table

PostgreSQL's COPY protocol is blazing fast. Maybe load data to a temp table first, then UNWIND in smaller batches?

```python
cursor.copy_from(buffer, "staging", columns=["id", "properties"])
for batch in fetch_batches("staging", size=200):
    cursor.execute(build_unwind_query(batch))
```

**Result:** COPY is fast, but we still pay Cypher overhead per batch. At 50k nodes: **timeout**. Dead end.

### The Breakthrough: Direct SQL

Then we looked at how AGE actually stores data.

**AGE is just PostgreSQL tables.** Each label gets its own table:
- `"my_graph"."Person"` for Person nodes  
- `"my_graph"."KNOWS"` for KNOWS edges

The Cypher layer is convenient but slow. What if we skip it entirely?

```python
# COPY to staging (fast)
cursor.copy_from(buffer, "staging", columns=["id", "properties"])

# INSERT directly into AGE's table (fast!)
cursor.execute("""
    INSERT INTO "graph"."Person" (id, properties)
    SELECT 
        ag_catalog._graphid(label_id, nextval(seq)),
        properties::agtype
    FROM staging
    WHERE NOT EXISTS (...)
""")
```

**Result:** ~35,000 nodes/second. At 50k nodes: **1.1 seconds**.

**That's 1,200x faster than UNWIND batching.**

---

## Quick Start

```bash
cd age-bulk-insert-sandbox
docker compose up -d
sleep 5
uv sync
uv run python -m benchmarks.run_all --sizes 100,1000,5000
```

---

## Full Benchmark Results

```
                                       AGE Bulk Insert Benchmark Results                                       
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ Strategy            ┃ 100 nodes ┃ 500 nodes ┃ 1000 nodes ┃ 5000 nodes ┃ 10000 nodes ┃ 50000 nodes ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ 0. Individual MERGE │      54ms │     336ms │      943ms │     15.65s │      57.43s │     22m 45s │
│ 1. UNWIND MERGE     │      27ms │     362ms │      1.26s │     31.07s │      2m 19s │     58m 52s │
│ 2. COPY + UNWIND    │      42ms │     893ms │      3.75s │     1m 34s │      6m 34s │     timeout │
│ 3. Direct SQL       │      14ms │      24ms │       32ms │      138ms │       401ms │       1.11s │
└─────────────────────┴───────────┴───────────┴────────────┴────────────┴─────────────┴─────────────┘
```

Notice: Strategies 1 and 2 are *slower* than the baseline at scale. Batching through Cypher doesn't help—Cypher itself is the bottleneck.

---

## When to Use What

| Situation | Recommendation |
|-----------|---------------|
| Interactive CRUD (1-10 entities) | Individual MERGE—simple and fast enough |
| Small batches (10-100 entities) | UNWIND—one roundtrip, readable code |
| Bulk loading (1000+ entities) | **Direct SQL**—nothing else comes close |

---

## Tradeoffs of Direct SQL

You're bypassing AGE's abstraction layer. That means:

1. **Fragility** — AGE's internal tables could change between versions
2. **Manual graphid management** — You compute IDs via `_graphid(label_id, sequence)`
3. **Lazy label creation** — Label tables don't exist until the first entity; create one via Cypher first

This implementation includes safeguards:
- Validates label names (prevents SQL injection)
- Detects duplicate IDs in batch
- Fails fast on orphaned edges (instead of silently dropping them)
- Escapes COPY data properly

**Use Direct SQL for one-time bulk loads when you control the AGE version.**

---

## How AGE Stores Data

For the curious, here's what's under the hood:

```sql
-- Graph metadata
SELECT * FROM ag_catalog.ag_graph;
SELECT * FROM ag_catalog.ag_label WHERE graph = (SELECT graphid FROM ag_catalog.ag_graph WHERE name = 'my_graph');

-- Nodes: each label is a table inheriting from _ag_label_vertex
SELECT * FROM "my_graph"."Person";

-- Edges: each label is a table inheriting from _ag_label_edge  
SELECT * FROM "my_graph"."KNOWS";  -- has start_id, end_id columns
```

Key functions:
- `ag_catalog._graphid(label_id, seq_value)` — compute a graphid
- `ag_catalog.agtype_object_field_text_agtype(props, '"id"')` — extract property

---

## Project Structure

```
src/strategies/
├── s0_individual_merge.py   # Baseline
├── s1_unwind_merge.py       # Batched Cypher
├── s2_copy_unwind.py        # COPY + batched Cypher
└── s3_direct_sql.py         # The fast one

benchmarks/
├── run_all.py               # Benchmark runner
└── visualize.py             # Chart generation
```

## References

- [Apache AGE Documentation](https://age.apache.org/age-manual/master/index.html)
- [AGE GitHub](https://github.com/apache/age)
- [PostgreSQL COPY Protocol](https://www.postgresql.org/docs/current/sql-copy.html)

## License

MIT

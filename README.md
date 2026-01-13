# Optimizing Bulk Insert with Apache AGE

While building our experimental knowledge graph platform, [Kartograph](https://github.com/openshift-hyperfleet/kartograph), we ran into significant performance issues when inserting >10K nodes into [Apache AGE](https://age.apache.org/) (a PostgreSQL graph extension).

This repo documents our journey optimizing bulk loading, moving through four strategies from simple to complex. We also provide a benchmark script so you can reproduce the results on your own hardware.

---

## The Approaches

### Strategy 0: Individual MERGE

The straightforward approach—one Cypher query per node:

```python
for node in nodes:
    cursor.execute(f"""
        SELECT * FROM cypher('graph', $$ 
            MERGE (n:Person {{id: '{node.id}'}}) SET n = {props}
        $$) AS (n agtype)
    """)
```

This works fine for small datasets. At ~1,500 nodes/second, 11k nodes takes about 7 seconds. The problem is that performance degrades as the graph grows—at 50k nodes, this takes over 20 minutes.

### Strategy 1: UNWIND Batching

Cypher's UNWIND lets you process an array of items in a single query:

```python
items = "[{id: '1', name: 'Alice'}, {id: '2', name: 'Bob'}, ...]"
cursor.execute(f"""
    SELECT * FROM cypher('graph', $$ 
        UNWIND {items} AS item
        MERGE (n:Person {{id: item.id}}) SET n = item
    $$) AS (n agtype)
""")
```

This reduces roundtrips to one, which helps for moderate batch sizes. However, at larger scales the Cypher parser becomes the bottleneck—query strings with thousands of items take longer to parse than the individual approach. At 50k nodes, this actually takes longer than Strategy 0.

### Strategy 2: COPY + UNWIND

PostgreSQL's COPY protocol can load data into a staging table very quickly. We tried combining this with batched UNWIND queries:

```python
cursor.copy_from(buffer, "staging", columns=["id", "properties"])
for batch in fetch_batches("staging", size=200):
    cursor.execute(build_unwind_query(batch))
```

The COPY step is fast, but we're still running Cypher for each batch. This ended up slower than both previous approaches because we're paying staging table overhead on top of Cypher overhead.

### Strategy 3: Direct SQL

AGE stores graph data in regular PostgreSQL tables—each label gets its own table (e.g., `"graph"."Person"`) that inherits from `_ag_label_vertex`. You can query these tables directly:

```sql
SELECT * FROM ag_catalog.ag_label;  -- label metadata (id, sequence name)
SELECT * FROM "my_graph"."Person";  -- actual node data
```

Node tables have two columns: `id` (a graphid) and `properties` (agtype). Since we know the schema, we can skip Cypher and INSERT directly:

```python
cursor.copy_from(buffer, "staging", columns=["id", "properties"])
cursor.execute("""
    INSERT INTO "graph"."Person" (id, properties)
    SELECT 
        ag_catalog._graphid(label_id, nextval(seq)),  -- compute graphid
        properties::agtype
    FROM staging
    WHERE NOT EXISTS (...)
""")
```

This runs at ~35,000 nodes/second—roughly 100x faster than the Cypher-based approaches.

---

## Results

| Approach | 10k nodes | Notes |
|----------|-----------|-------|
| Individual MERGE | 57s | N roundtrips, N query parses |
| UNWIND batch | 2m 19s | Single query, but parser struggles with large inputs |
| COPY + UNWIND | 6m 34s | Fast staging, but still Cypher-bound |
| Direct SQL | 0.4s | Bypasses Cypher, writes to AGE tables directly |

<details>
<summary>Full benchmark table</summary>

```
┏━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ Strategy            ┃ 100 nodes ┃ 500 nodes ┃ 1000 nodes ┃ 5000 nodes ┃ 10000 nodes ┃ 50000 nodes ┃
┡━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ 0. Individual MERGE │      54ms │     336ms │      943ms │     15.65s │      57.43s │     22m 45s │
│ 1. UNWIND MERGE     │      27ms │     362ms │      1.26s │     31.07s │      2m 19s │     58m 52s │
│ 2. COPY + UNWIND    │      42ms │     893ms │      3.75s │     1m 34s │      6m 34s │     timeout │
│ 3. Direct SQL       │      14ms │      24ms │       32ms │      138ms │       401ms │       1.11s │
└─────────────────────┴───────────┴───────────┴────────────┴────────────┴─────────────┴─────────────┘
```

</details>

---

## When to Use What

| Situation | Recommendation |
|-----------|---------------|
| Interactive operations (1-10 entities) | Individual MERGE is simple and fast enough |
| Moderate batches (10-100 entities) | UNWIND reduces roundtrips without parser issues |
| Bulk loading (1000+ entities) | Direct SQL is the only approach that scales well |

---

## Tradeoffs of Direct SQL

Writing directly to AGE's tables bypasses its abstraction layer:

- **Version coupling**: The internal table structure could change between AGE versions
- **Manual ID management**: You need to compute graphids using `_graphid(label_id, sequence_value)`
- **Label bootstrapping**: Label tables are created lazily—you need to create the first entity via Cypher

This implementation includes safeguards for common issues:
- Validates label names to prevent SQL injection
- Detects duplicate IDs in batches
- Fails on orphaned edges rather than silently dropping them
- Properly escapes COPY data

Direct SQL makes sense for bulk loading when you control the AGE version. For interactive operations or when cross-version compatibility matters, stick with Cypher.

---

## Try It Yourself

```bash
docker compose up -d
uv sync
uv run python -m benchmarks.run_all --sizes 100,1000,5000
```

---

## Project Structure

```
src/strategies/
├── s0_individual_merge.py
├── s1_unwind_merge.py
├── s2_copy_unwind.py
└── s3_direct_sql.py

benchmarks/
├── run_all.py
└── visualize.py
```

## References

- [Apache AGE Documentation](https://age.apache.org/age-manual/master/index.html)
- [AGE GitHub](https://github.com/apache/age)
- [PostgreSQL COPY Protocol](https://www.postgresql.org/docs/current/sql-copy.html)

## License

MIT

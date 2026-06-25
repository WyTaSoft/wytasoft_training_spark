# UC4 — Skewed Join & Broadcast

> 🇫🇷 Version française : [`README.fr.md`](./README.fr.md)

## Context

A job aggregates `skewed_transactions` per client (total amount + transaction count), joins the
result with a small `clients` dimension, and reports total amount and average transactions per
client, grouped by the client's geography.

Starter code: [`pyspark/w004.py`](./pyspark/w004.py).

## What the job does

1. Reads `skewed_transactions` and `clients` with `inferSchema=true`.
2. Aggregates `sum(amount)`, `count(*)` per `client_id`.
3. Left-joins with `clients`.
4. Groups by geography and reports `sum(total_amount)`, `avg(tx_count)`.

## Two issues to fix

- **Heavy skew.** Two clients dominate the data (`C00001` ≈ 49 %, `C00002` ≈ 34 % of all rows).
  The per-client aggregation and any shuffle join concentrate work on a few tasks — one runs far
  longer than the rest.
- **A bug.** The job groups by `country`, but after the per-client aggregation there is **no
  `country` column** (it was dropped). The `clients` dimension's geographic column is `region`.
  The report should group by `region`.

## Your mission

Produce the (corrected) report, but cut the shuffle and balance the skew. Use the Spark UI
(SQL plan + Stages task summary).

### Things to investigate

- **Broadcast the small dimension.** `clients` is tiny (~1000 rows). A `broadcast(...)` join avoids
  shuffling the aggregated fact side — Broadcast Hash Join instead of Sort-Merge Join.
- **AQE (on by default in Spark 3.5).** Coalesces shuffle partitions, can switch to broadcast at
  runtime, and splits skewed partitions (`spark.sql.adaptive.skewJoin.enabled`). Confirm it's on.
- **Why the first aggregation survives skew.** `sum`/`count` are combinable, so Spark does a
  partial (map-side) aggregation before the shuffle — the hot key is largely pre-reduced. Discuss
  when an aggregation is *not* combinable and skew bites harder.
- **Salting (optional).** For a genuine **shuffle join** where one key dominates, salt the key.
  Here broadcast removes the shuffle, so salting isn't needed — but know the technique.
- **Explicit schema; drop dead columns.** Skip `inferSchema`; the starter computes a `year` column
  it never uses.

### Optimization goals (measurable)

| Metric                          | Where to see it                              |
|---------------------------------|----------------------------------------------|
| Join strategy                   | Spark UI → SQL (Broadcast vs SortMerge)      |
| Max vs median task time         | Spark UI → Stages → task summary (skew)      |
| Shuffle read/write bytes        | Spark UI → Stages                            |
| Wall-clock time                 | Job duration                                 |

## Hints

<details><summary>Hint 1 — fix the grouping column</summary>

Group by `region` (from `clients`), not `country`.
</details>

<details><summary>Hint 2 — broadcast the dimension</summary>

`from pyspark.sql.functions import broadcast` then join on `broadcast(clients)`.
</details>

<details><summary>Hint 3 — confirm AQE</summary>

`spark.sql.adaptive.enabled` and `spark.sql.adaptive.skewJoin.enabled` are `true` by default in
3.5. Check the Stages task summary to see the hot task before/after.
</details>

<details><summary>Hint 4 — explicit schema, drop the dead column</summary>

Declare a `StructType`; remove the unused `year` derivation.
</details>

## Deliverable

An optimized job producing the per-`region` report, using a broadcast join (no big-side shuffle),
AQE enabled, balanced task times, and an explicit schema. Be ready to show the join-strategy change
in the SQL tab and the reduced task-time skew in Stages.

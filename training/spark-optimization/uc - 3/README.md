# UC3 — Reading & Caching a Reused DataFrame

> 🇫🇷 Version française : [`README.fr.md`](./README.fr.md)

## Context

A job reads a `sales` CSV, filters it to a list of 10 priority products, and from that **same
filtered DataFrame** computes two reports: total sales per category, and the top-5 clients by
sales.

Starter code: [`pyspark/w003.py`](./pyspark/w003.py) (Scala: [`scala/w003.scala`](./scala/w003.scala)).

## What the job does

1. Reads `sales` with `inferSchema=true`.
2. Filters `product_id IN (priority list)`.
3. From the filtered DataFrame: (a) sum per `category`, (b) top-5 clients by total sales.
4. `show()`s both.

## The problem

The filtered DataFrame is used by **two** actions (`show()` on each report). Because it isn't
cached, the read + filter runs **twice**. `inferSchema` adds yet another full pass.

## Your mission

Keep both reports identical, but read and filter **once**. Use the Spark UI (Jobs + Storage).

### Things to investigate

- **`inferSchema`.** It triggers an extra scan just to guess types. Replace it with an explicit
  schema.
- **Column pruning.** The reports use `product_id`, `category`, `client_id`, `amount` — not `date`.
- **Caching the reused node.** `filteredDf` feeds two `show()`s. Cache it and materialize once so
  the second report is served from memory.
- **Is caching even worth it here?** Only two reuses — discuss the trade-off vs just accepting two
  scans, and when caching wins.

### Optimization goals (measurable)

| Metric                       | Where to see it                       |
|------------------------------|---------------------------------------|
| Number of source scans / jobs| Spark UI → Jobs / SQL scan nodes      |
| Cached entry                 | Spark UI → Storage tab                |
| Columns read                 | Spark UI → SQL scan node              |
| Wall-clock time              | Sum of cell durations                 |

## Hints

<details><summary>Hint 1 — explicit schema + pruning</summary>

Declare a `StructType`, and `select` only the four columns the reports use.
</details>

<details><summary>Hint 2 — cache the filtered DataFrame</summary>

`filteredDf.cache()` then one action (e.g. the first report) populates the cache; the second
report reads from memory.
</details>

<details><summary>Hint 3 — clean up</summary>

`unpersist()` after both reports if you keep going in the same session.
</details>

## Deliverable

An optimized job producing the same two reports, where the source is read and filtered once
(cached), with an explicit schema and only the needed columns.

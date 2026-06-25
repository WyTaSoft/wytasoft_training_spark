# UC1 — Reading & Basic Aggregation

> 🇫🇷 Version française : [`README.fr.md`](./README.fr.md)

## Context

A job reads a CSV folder of `transactions`, keeps the rows with a non-empty `amount`, casts it to
`double`, sums the amount per `(client_id, transaction_type)`, and writes the result as Parquet.

Starter code: [`pyspark/w001.py`](./pyspark/w001.py) (Scala: [`scala/w001.scala`](./scala/w001.scala)).

## What the job does

1. Reads all columns of the CSV as **strings** (no schema).
2. Filters `amount != ""`, then casts `amount` to double.
3. Aggregates `sum(amount)` per `client_id`, `transaction_type`.
4. Writes Parquet.

## Your mission

Keep the **output identical**, but make the read & transform cheaper. Use the Spark UI (SQL scan
node) to see how much data is read.

### Things to investigate

- **No schema.** Every column is read as a string and `amount` is cast later. What does an
  explicit `StructType` buy you (correct types up front, no string→double cast pass, and no
  `inferSchema` scan if you ever turn it on)?
- **Reading more than you need.** The job uses only 3 of the 5 columns. Look up *column pruning*
  (`select`).
- **Empty vs null.** `filter(amount != "")` then cast — can you express the filter once, after a
  typed read (`amount IS NOT NULL`), and get the same rows?
- **Output files.** The result is tiny (one row per client/type). How many output files does the
  write produce, and are there too many small files?

### Optimization goals (measurable)

| Metric                      | Where to see it                       |
|-----------------------------|---------------------------------------|
| Columns read                | Spark UI → SQL → scan node            |
| Number of stages / passes   | Spark UI → Jobs                       |
| Output file count           | The Parquet target folder             |
| Wall-clock time             | Cell / job duration                   |

## Hints

<details><summary>Hint 1 — explicit schema</summary>

Declare a `StructType` with `amount` as `double`. No per-row string cast, correct types from the
start.
</details>

<details><summary>Hint 2 — read only what you need</summary>

`select("client_id", "transaction_type", "amount")` before aggregating.
</details>

<details><summary>Hint 3 — one filter</summary>

With a typed read, `filter(col("amount").isNotNull())` replaces the empty-string filter + cast.
</details>

<details><summary>Hint 4 — avoid small files</summary>

The aggregated result is small — `coalesce()` (or partition by `transaction_type`) so you don't
write hundreds of tiny Parquet files.
</details>

## Deliverable

An optimized job producing the same `(client_id, transaction_type, total_amount)` Parquet, reading
fewer columns with correct types in a single pass and a sensible number of output files.

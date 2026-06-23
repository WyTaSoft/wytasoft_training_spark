# UC01 — Reading Data Efficiently

## Context

You work on a bank's data team. A colleague wrote a notebook that reads a large CSV dataset of
banking transactions, then produces a small daily report (total transaction amount per country
for a single day).

The notebook **works**, but it is slow and expensive. Your task is to make the **read phase**
efficient — that's where most of the time and cost is being spent.

The starter code is in [`starter_unoptimized.py`](./starter_unoptimized.py).

## What the job does

1. Reads a CSV folder of transactions.
2. Filters to one transaction date and one set of countries.
3. Aggregates total transaction amount per country.
4. Displays the result.

## Your mission

Keep the **output identical**, but optimize how the data is read and processed.
Use the Spark UI (Jobs / Stages / SQL tab) to measure before and after.

### Things to investigate

- **Schema inference.** The reader scans the whole file just to guess column types. What does
  `inferSchema=True` cost on a large file? What are the alternatives?
- **File format.** CSV is row-based, uncompressed, not splittable when gzipped, and carries no
  statistics. Is there a better format for repeated reads?
- **Reading more than you need.** The job needs 3 columns out of ~10 and 1 day out of many.
  How much data is actually scanned? Look up *column pruning* and *predicate/partition pruning*.
- **Filtering after the fact.** Where in the plan does the filter happen relative to the read?
- **Repeated work.** Is the same DataFrame read or computed more than once?
- **`count()` / `collect()` / `print` debugging.** Are there actions that trigger extra jobs?

### Optimization goals (measurable)

| Metric                       | Where to see it                         |
|------------------------------|-----------------------------------------|
| Wall-clock time              | Cell run time / Spark UI job duration   |
| Number of jobs & stages      | Spark UI → Jobs                         |
| Bytes / files read           | Spark UI → SQL → scan node              |
| Rows read vs rows needed     | Spark UI → SQL → "number of output rows"|

## Hints (reveal progressively)

<details>
<summary>Hint 1 — schema</summary>

Don't pay for `inferSchema`. Define an explicit `StructType`, or read once from a format that
already stores the schema (Parquet/Delta).
</details>

<details>
<summary>Hint 2 — only read what you need</summary>

`select` the 3 columns you actually use, and `filter` on the date *before* aggregating. With a
columnar format Spark can prune columns and skip row groups using file statistics.
</details>

<details>
<summary>Hint 3 — format & layout</summary>

Convert the source to Parquet or Delta once, partitioned by `transaction_date`. Subsequent reads only
touch the relevant partition (partition pruning) and the relevant columns (column pruning).
</details>

<details>
<summary>Hint 4 — avoid extra actions</summary>

Each `count()`/`collect()`/`show()` is a separate job. Remove debug actions from the hot path.
</details>

## Deliverable

An optimized notebook that produces the same report with fewer bytes read, fewer jobs, and lower
wall-clock time. Be ready to explain *which* change gave *which* improvement.

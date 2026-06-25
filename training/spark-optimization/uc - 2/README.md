# UC2 — Repeated Computation & an Expensive Join

> 🇫🇷 Version française : [`README.fr.md`](./README.fr.md)

## Context

A fraud-detection job reads a `transactions` table once, then applies **three rules** to flag
suspicious clients:

1. Withdrawals over a rolling 7-day window summing to > €10,000.
2. More than 5 foreign (`country != FR`) transactions in a rolling 3-day window.
3. "Impossible travel": a movement of > 1000 km in under 2 hours.

It unions the suspect client ids from all three rules and writes them out.

Starter code: [`pyspark/w002.py`](./pyspark/w002.py) (Scala: [`scala/w002.scala`](./scala/w002.scala)).

## The two problems

- **Repeated computation.** The base `df` feeds rule 1, rule 2 **and** rule 3. Nothing is cached,
  so the read + parse runs from scratch for each rule (and again for the final write).
- **An expensive self-join.** Rule 3 self-joins the transactions on `client_id` with a time
  predicate to compare every pair of transactions — an `O(n²)`-per-client blow-up and a large
  shuffle.

## Your mission

Keep the **set of suspect clients equivalent**, but stop recomputing the source and replace the
self-join with something that scales. Use the Spark UI (Jobs + SQL + Stages).

### Things to investigate

- **Caching the reused node.** `df` is read once logically but recomputed per action. Cache the
  parsed, column-pruned DataFrame and materialize it once.
- **Self-join vs window.** Rule 3 compares each transaction to others of the same client within
  2 hours. A **window function** (`lag` over `client_id` ordered by `timestamp`) compares each
  transaction to the **previous** one — no self-join, one shuffle, linear in rows. Discuss the
  semantic difference (consecutive movements vs all pairs) and why consecutive is the realistic
  impossible-travel signal.
- **Column pruning.** Each rule needs only a few columns. Don't carry all of them through shuffles.
- **Explicit schema.** Skip `inferSchema` (an extra full scan).

### Optimization goals (measurable)

| Metric                          | Where to see it                       |
|---------------------------------|---------------------------------------|
| Number of source scans / jobs   | Spark UI → Jobs / SQL scan nodes      |
| Cached entry                    | Spark UI → Storage tab                |
| Shuffle read/write bytes (rule 3)| Spark UI → Stages                    |
| Wall-clock time                 | Whole-job duration                    |

## Hints

<details><summary>Hint 1 — cache the base</summary>

After an explicit-schema read and a `select` of the columns the rules need, `persist` it and run
one action so all three rules read from cache.
</details>

<details><summary>Hint 2 — kill the self-join</summary>

`Window.partitionBy("client_id").orderBy("timestamp")` + `lag(...)` to get the previous position
and time. Compute the haversine distance to the previous point and the seconds elapsed; flag rows
where elapsed < 7200 and distance > 1000.
</details>

<details><summary>Hint 3 — unpersist</summary>

Release the cache once the suspects are written.
</details>

## Deliverable

An optimized job producing the equivalent suspect-client set, with the source cached (scanned
once) and rule 3 implemented with a window instead of a self-join. Be ready to show the Storage
tab and the drop in shuffle for rule 3.

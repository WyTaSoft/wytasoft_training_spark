# UC03 — Caching & Repeated Computation

> 🇫🇷 Version française : [`README.fr.md`](./README.fr.md)

## Context

Same bank. A colleague builds a small "daily KPIs" notebook from the **transactions** table.
The transformation is non-trivial (parse, clean, derive a few columns), and the **same derived
DataFrame is reused several times** — for a total, a per-country breakdown, a per-channel
breakdown, and a couple of `count()` sanity checks.

Because nothing is cached, Spark **recomputes the whole pipeline from the source files every
time** an action runs. The notebook **works** but does far more work than necessary.

The starter code is in [`starter_unoptimized.py`](./starter_unoptimized.py).

## What the job does

1. Reads `transactions`, cleans it, and derives a few columns (a moderately expensive transform).
2. Filters to "POSTED" transactions for one month.
3. From that **same** DataFrame, computes: a grand total, a per-country report, a per-channel
   report, and several `count()` checks.

## Your mission

Keep the **outputs identical**, but stop recomputing the shared pipeline. Use the **Spark UI**
(Jobs tab + Storage tab) to count how many times the source is scanned before and after.

### Things to investigate

- **Lazy evaluation.** Transformations are lazy; only **actions** trigger work. Every
  `count()` / `display()` / `collect()` re-runs the lineage from the source unless something is
  materialized. How many actions does the starter fire, and how many full scans does that cause?
- **`cache()` / `persist()`.** What exactly does caching do, and *when* (it's lazy too — the
  first action populates the cache). Which `StorageLevel` makes sense here?
- **Where to cache.** Cache the *reused* node — after the expensive transform/filter, not the raw
  read. Caching too early or caching something used once wastes memory.
- **Releasing memory.** What happens if you cache and never `unpersist()`? When should you free it?
- **Alternatives to cache.** Sometimes the right answer is *not* caching: a single combined
  aggregation, or writing an intermediate Delta table for downstream reuse. When is each better?
- **Don't count to debug.** Each `count()` is a full job. Are those checks worth their cost?

### Optimization goals (measurable)

| Metric                          | Where to see it                              |
|---------------------------------|----------------------------------------------|
| Number of jobs / source scans   | Spark UI → Jobs (and SQL scan nodes)         |
| Bytes read (sum across jobs)    | Spark UI → SQL → scan nodes                  |
| Cached size / fraction          | Spark UI → **Storage** tab                   |
| Wall-clock time (whole notebook)| Sum of cell run times                        |

## Hints (reveal progressively)

<details>
<summary>Hint 1 — find the reused DataFrame</summary>

Identify the node that every downstream report starts from (the cleaned + filtered transactions).
That single DataFrame is recomputed once per action. It's the caching candidate.
</details>

<details>
<summary>Hint 2 — cache it, then materialize once</summary>

Call `.cache()` (or `.persist(StorageLevel.MEMORY_AND_DISK)`) on that DataFrame, then trigger
**one** action to populate the cache. Subsequent reports read from memory, not from the source.
Check the **Storage** tab to confirm it's cached.
</details>

<details>
<summary>Hint 3 — drop the debug counts (or do them once)</summary>

Each `count()` is a separate full job. Remove them from the hot path, or compute all the sanity
metrics in a single pass with one aggregation.
</details>

<details>
<summary>Hint 4 — clean up, and consider not caching at all</summary>

`unpersist()` when you're done so you don't hold memory. If the derived data is reused across
*notebooks/jobs* (not just within this one), writing an intermediate **Delta** table is often
better than an in-memory cache. If it's used only once, don't cache it.
</details>

## Deliverable

An optimized notebook producing the same total / per-country / per-channel reports, where the
shared pipeline is computed **once** (cached or materialized), with the source scanned far fewer
times. Be ready to show the drop in jobs/scans in the Spark UI and the cached entry in the
Storage tab.

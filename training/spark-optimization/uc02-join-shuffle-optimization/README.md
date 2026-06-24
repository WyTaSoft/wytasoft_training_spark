# UC02 — Joins & Shuffles (Spark 3.5)

> 🇫🇷 Version française : [`README.fr.md`](./README.fr.md)

## Context

Same bank, new job. A colleague enriches the large **transactions** fact table with the small
**customers** dimension (to get each customer's `segment`), then reports total transaction amount
per segment for a given month.

The join is the expensive part: it triggers a big **shuffle**, and one customer is much more
active than the others, so the work is **skewed**. The notebook **works** but is slow.

The starter code is in [`starter_unoptimized.py`](./starter_unoptimized.py).

## What the job does

1. Reads `transactions` (large fact) and `customers` (small dimension).
2. Joins them on `customer_id`.
3. Filters to one month.
4. Aggregates total amount per customer `segment`.

## Your mission

Keep the **output identical**, but cut the shuffle cost and fix the skew.
Use the **Spark UI** (SQL plan + Stages) to see the join strategy and per-task skew.

### Things to investigate

- **Join strategy.** The starter disables broadcast and AQE to mimic a naive setup, so Spark uses
  a **Sort-Merge Join** that shuffles *both* sides. The `customers` table is tiny — what join
  strategy avoids shuffling the big side entirely?
- **AQE (Adaptive Query Execution).** It is **on by default in Spark 3.5**. What does it do for
  you automatically: coalescing shuffle partitions, switching to broadcast at runtime, splitting
  skewed partitions? The starter turns it off — turn it back on and observe.
- **Skew.** One `customer_id` is a hot key (and there are some `NULL` keys). One task runs far
  longer than the others. How does AQE skew-join help? When would you **salt** the key instead?
- **Shuffle volume.** Are you shuffling columns you don't need? Filtering rows you'll drop anyway?
- **`spark.sql.shuffle.partitions`.** The default is 200. On small/medium data that's often far
  too many tiny tasks — what handles this for you in 3.5?

### Optimization goals (measurable)

| Metric                          | Where to see it                              |
|---------------------------------|----------------------------------------------|
| Join strategy                   | Spark UI → SQL → join node (SortMerge vs Broadcast) |
| Shuffle read/write bytes        | Spark UI → Stages                            |
| Max vs median task time         | Spark UI → Stages → task summary (skew)      |
| Number of shuffle partitions    | Spark UI → SQL → exchange node               |
| Wall-clock time                 | Cell run time / Spark UI job duration        |

## Hints (reveal progressively)

<details>
<summary>Hint 1 — let the small side broadcast</summary>

The `customers` dimension is small. Either wrap it in `broadcast(...)`, or just leave
`spark.sql.autoBroadcastJoinThreshold` at its default (10 MB) and let Spark choose a
**Broadcast Hash Join** — no shuffle of the big fact table.
</details>

<details>
<summary>Hint 2 — turn AQE back on (it's the 3.5 default)</summary>

`spark.sql.adaptive.enabled=true` (default). It coalesces the 200 shuffle partitions down to a
sensible number, can convert a sort-merge join to broadcast at runtime, and splits skewed
partitions when `spark.sql.adaptive.skewJoin.enabled=true` (also default).
</details>

<details>
<summary>Hint 3 — shrink the shuffle before it happens</summary>

`select` only `customer_id` + the columns you aggregate, and `filter` to the target month
*before* the join. Less data crosses the network. Also drop `NULL` join keys early — they never
match and can pile into one task.
</details>

<details>
<summary>Hint 4 — when AQE skew-join isn't enough, salt</summary>

If a single key is so hot that even AQE struggles, add a salt: append a small random suffix to the
key on the fact side and explode the dimension across the same salt values, join on
`(key, salt)`, then aggregate. Use this only when measurement shows AQE skew handling is
insufficient.
</details>

## Deliverable

An optimized notebook with the same per-segment report, using a broadcast join (no big-side
shuffle), AQE enabled, reduced shuffle volume, and balanced task times. Be ready to explain the
plan change in the Spark UI SQL tab.

# Spark Optimization — Training Use Cases (PySpark / Databricks)

A series of hands-on use cases for a Spark optimization training session.

Each use case follows the same pattern:

1. **`starter_unoptimized.py`** — working but deliberately *non-optimized* code. This is what
   you hand to the trainees. Their job is to make it faster / cheaper / more robust.
2. **`README.md`** — the problem statement, what to look at, and the optimization goals.
3. **`solution.py`** — *instructor copy*. One reasonable optimized version with explanations.
   Don't distribute this until after the exercise.

> All notebooks are saved in **Databricks notebook source** format (`# Databricks notebook source`
> with `# COMMAND ----------` cell separators). You can import the `.py` files directly into a
> Databricks workspace (Workspace → Import → File).

## Use cases

| #    | Topic                          | Folder                          |
|------|--------------------------------|---------------------------------|
| UC01 | Reading data efficiently       | `uc01-reading-optimization/`    |

More to come (writing/partitioning, joins & shuffles, caching, skew, UDFs vs native, etc.).

## How to run the exercise

1. Import the `starter_unoptimized.py` notebook into Databricks.
2. Trainees run it as-is and inspect the **Spark UI** (Jobs / Stages / SQL) to see what's slow.
3. They iterate on the code, re-running and comparing wall-clock time, number of tasks,
   bytes read, and number of jobs/stages.
4. Debrief with `solution.py`.

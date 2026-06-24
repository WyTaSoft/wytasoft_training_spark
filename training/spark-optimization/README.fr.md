# Optimisation Spark — Cas d'usage de formation (PySpark / Databricks)

Une série de cas d'usage pratiques pour une session de formation sur l'optimisation Spark.

Chaque cas d'usage suit le même schéma :

1. **`starter_unoptimized.py`** — du code fonctionnel mais volontairement *non optimisé*.
   C'est ce que l'on distribue aux participants. Leur mission : le rendre plus rapide, moins
   coûteux et plus robuste.
2. **`README.md`** — l'énoncé du problème, les points à examiner et les objectifs d'optimisation.
3. **`solution.py`** — *copie formateur*. Une version optimisée raisonnable, avec explications.
   À ne pas distribuer avant la correction.

> Tous les notebooks sont enregistrés au format **Databricks notebook source**
> (`# Databricks notebook source` avec des séparateurs de cellules `# COMMAND ----------`).
> Vous pouvez importer directement les fichiers `.py` dans un workspace Databricks
> (Workspace → Import → File).

## Cas d'usage

| #    | Thème                              | Dossier                             |
|------|------------------------------------|-------------------------------------|
| UC01 | Lecture efficace des données       | `uc01-reading-optimization/`        |
| UC02 | Jointures & shuffles (Spark 3.5)   | `uc02-join-shuffle-optimization/`   |

À venir (écriture/partitionnement, mise en cache, skew, UDF vs natif, etc.).

## Déroulé de l'exercice

1. Importer le notebook `starter_unoptimized.py` dans Databricks.
2. Les participants l'exécutent tel quel et inspectent la **Spark UI** (Jobs / Stages / SQL)
   pour repérer ce qui est lent.
3. Ils itèrent sur le code, ré-exécutent et comparent : temps d'exécution (wall-clock),
   nombre de tâches, octets lus, nombre de jobs/stages.
4. Débrief avec `solution.py`.

## Thème des données

Les jeux de données utilisent un contexte **bancaire** : des transactions
(`transaction_id`, `transaction_date`, `country`, `customer_id`, `account_id`, `amount`,
`currency`, `transaction_type`, `channel`, `status`).

> Version anglaise : [`README.md`](./README.md)

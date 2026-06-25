# UC4 — Jointure déséquilibrée (skew) & broadcast

> 🇬🇧 English version: [`README.md`](./README.md)

## Contexte

Un job agrège `skewed_transactions` par client (montant total + nombre de transactions), joint le
résultat avec une petite dimension `clients`, et produit le montant total et le nombre moyen de
transactions par client, groupés par la géographie du client.

Code de départ : [`pyspark/w004.py`](./pyspark/w004.py).

## Ce que fait le job

1. Lit `skewed_transactions` et `clients` avec `inferSchema=true`.
2. Agrège `sum(amount)`, `count(*)` par `client_id`.
3. Jointure à gauche avec `clients`.
4. Groupe par géographie et produit `sum(total_amount)`, `avg(tx_count)`.

## Deux problèmes à corriger

- **Fort skew.** Deux clients dominent les données (`C00001` ≈ 49 %, `C00002` ≈ 34 % des lignes).
  L'agrégation par client et toute jointure par shuffle concentrent le travail sur quelques
  tâches — l'une dure bien plus longtemps que les autres.
- **Un bug.** Le job groupe par `country`, mais après l'agrégation par client il n'y a **aucune
  colonne `country`** (elle a été supprimée). La colonne géographique de la dimension `clients` est
  `region`. Le rapport doit grouper par `region`.

## Votre mission

Produisez le rapport (corrigé), mais réduisez le shuffle et équilibrez le skew. Utilisez la
Spark UI (plan SQL + résumé des tâches dans Stages).

### Pistes à explorer

- **Broadcaster la petite dimension.** `clients` est minuscule (~1000 lignes). Une jointure
  `broadcast(...)` évite de shuffler le côté faits agrégé — Broadcast Hash Join au lieu de
  Sort-Merge Join.
- **AQE (activée par défaut en Spark 3.5).** Fusionne les partitions de shuffle, peut basculer en
  broadcast à l'exécution, et découpe les partitions déséquilibrées
  (`spark.sql.adaptive.skewJoin.enabled`). Vérifiez qu'elle est active.
- **Pourquoi la première agrégation résiste au skew.** `sum`/`count` sont combinables : Spark fait
  une agrégation partielle (côté map) avant le shuffle — la clé chaude est largement pré-réduite.
  Discutez des cas où une agrégation n'est *pas* combinable et où le skew frappe plus fort.
- **Salting (optionnel).** Pour une véritable **jointure par shuffle** où une clé domine, on salte
  la clé. Ici le broadcast supprime le shuffle, donc inutile — mais connaissez la technique.
- **Schéma explicite ; supprimer les colonnes mortes.** Évitez `inferSchema` ; le code de départ
  calcule une colonne `year` jamais utilisée.

### Objectifs d'optimisation (mesurables)

| Métrique                          | Où la voir                                   |
|-----------------------------------|----------------------------------------------|
| Stratégie de jointure             | Spark UI → SQL (Broadcast vs SortMerge)      |
| Temps max vs médian par tâche     | Spark UI → Stages → résumé des tâches (skew) |
| Octets de shuffle lus/écrits      | Spark UI → Stages                            |
| Temps d'exécution                 | Durée du job                                 |

## Indices

<details><summary>Indice 1 — corriger la colonne de regroupement</summary>

Groupez par `region` (de `clients`), pas par `country`.
</details>

<details><summary>Indice 2 — broadcaster la dimension</summary>

`from pyspark.sql.functions import broadcast` puis jointure sur `broadcast(clients)`.
</details>

<details><summary>Indice 3 — confirmer l'AQE</summary>

`spark.sql.adaptive.enabled` et `spark.sql.adaptive.skewJoin.enabled` sont à `true` par défaut en
3.5. Vérifiez le résumé des tâches dans Stages pour voir la tâche chaude avant/après.
</details>

<details><summary>Indice 4 — schéma explicite, supprimer la colonne morte</summary>

Déclarez un `StructType` ; retirez la dérivation `year` inutilisée.
</details>

## Livrable

Un job optimisé produisant le rapport par `region`, avec une jointure broadcast (pas de shuffle du
gros côté), l'AQE activée, des temps de tâche équilibrés et un schéma explicite. Soyez prêt à
montrer le changement de stratégie de jointure dans l'onglet SQL et la réduction du skew des temps
de tâche dans Stages.

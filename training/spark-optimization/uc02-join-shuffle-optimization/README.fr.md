# UC02 — Jointures & Shuffles (Spark 3.5)

> 🇬🇧 English version: [`README.md`](./README.md)

## Contexte

Même banque, nouveau job. Un collègue enrichit la grosse table de faits **transactions** avec la
petite dimension **customers** (pour récupérer le `segment` de chaque client), puis produit le
montant total des transactions par segment pour un mois donné.

La jointure est la partie coûteuse : elle déclenche un gros **shuffle**, et un client est bien
plus actif que les autres, ce qui rend le traitement **déséquilibré (skew)**. Le notebook
**fonctionne** mais il est lent.

Le code de départ se trouve dans [`starter_unoptimized.py`](./starter_unoptimized.py).

## Ce que fait le job

1. Lit `transactions` (grosse table de faits) et `customers` (petite dimension).
2. Les joint sur `customer_id`.
3. Filtre sur un mois.
4. Agrège le montant total par `segment` client.

## Votre mission

Conservez un **résultat identique**, mais réduisez le coût du shuffle et corrigez le skew.
Utilisez la **Spark UI** (plan SQL + Stages) pour observer la stratégie de jointure et le skew
par tâche.

### Pistes à explorer

- **Stratégie de jointure.** Le code de départ désactive le broadcast et l'AQE pour imiter une
  configuration naïve : Spark utilise donc une **Sort-Merge Join** qui shuffle les *deux* côtés.
  La table `customers` est minuscule — quelle stratégie évite de shuffler le gros côté ?
- **AQE (Adaptive Query Execution).** Elle est **activée par défaut dans Spark 3.5**. Que fait-elle
  automatiquement : fusion des partitions de shuffle, bascule vers un broadcast à l'exécution,
  découpage des partitions déséquilibrées ? Le code de départ la désactive — réactivez-la et
  observez.
- **Skew.** Un `customer_id` est une clé « chaude » (et il y a quelques clés `NULL`). Une tâche
  dure bien plus longtemps que les autres. En quoi le skew-join de l'AQE aide-t-il ? Quand
  faut-il plutôt **salter** la clé ?
- **Volume de shuffle.** Shufflez-vous des colonnes inutiles ? Des lignes que vous allez de toute
  façon supprimer ?
- **`spark.sql.shuffle.partitions`.** La valeur par défaut est 200. Sur des données petites/moyennes
  cela fait souvent beaucoup trop de petites tâches — qu'est-ce qui gère cela pour vous en 3.5 ?

### Objectifs d'optimisation (mesurables)

| Métrique                              | Où la voir                                   |
|---------------------------------------|----------------------------------------------|
| Stratégie de jointure                 | Spark UI → SQL → nœud de join (SortMerge vs Broadcast) |
| Octets de shuffle lus/écrits          | Spark UI → Stages                            |
| Temps max vs médian par tâche         | Spark UI → Stages → résumé des tâches (skew) |
| Nombre de partitions de shuffle       | Spark UI → SQL → nœud exchange               |
| Temps d'exécution (wall-clock)        | Durée de la cellule / job Spark UI           |

## Indices (à dévoiler progressivement)

<details>
<summary>Indice 1 — laisser le petit côté être broadcasté</summary>

La dimension `customers` est petite. Soit vous l'enveloppez dans `broadcast(...)`, soit vous
laissez `spark.sql.autoBroadcastJoinThreshold` à sa valeur par défaut (10 Mo) et Spark choisit une
**Broadcast Hash Join** — pas de shuffle de la grosse table de faits.
</details>

<details>
<summary>Indice 2 — réactiver l'AQE (défaut en 3.5)</summary>

`spark.sql.adaptive.enabled=true` (défaut). Elle fusionne les 200 partitions de shuffle vers un
nombre raisonnable, peut convertir une sort-merge join en broadcast à l'exécution, et découpe les
partitions déséquilibrées quand `spark.sql.adaptive.skewJoin.enabled=true` (également par défaut).
</details>

<details>
<summary>Indice 3 — réduire le shuffle avant qu'il n'arrive</summary>

Faites un `select` uniquement de `customer_id` + les colonnes à agréger, et un `filter` sur le mois
cible *avant* la jointure. Moins de données traversent le réseau. Supprimez aussi tôt les clés de
jointure `NULL` — elles ne matchent jamais et peuvent s'accumuler dans une seule tâche.
</details>

<details>
<summary>Indice 4 — quand le skew-join de l'AQE ne suffit pas, salter</summary>

Si une seule clé est tellement chaude que même l'AQE peine, ajoutez un « sel » : on ajoute un
petit suffixe aléatoire à la clé côté faits, et on duplique la dimension sur les mêmes valeurs de
sel, on joint sur `(clé, sel)`, puis on agrège. À utiliser seulement quand la mesure montre que la
gestion du skew par l'AQE est insuffisante.
</details>

## Livrable

Un notebook optimisé produisant le même rapport par segment, avec une broadcast join (pas de
shuffle du gros côté), l'AQE activée, un volume de shuffle réduit et des temps de tâche équilibrés.
Soyez prêt à expliquer le changement de plan dans l'onglet SQL de la Spark UI.

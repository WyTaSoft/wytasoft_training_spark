# UC01 — Lecture efficace des données

## Contexte

Vous travaillez dans l'équipe data d'une banque. Un collègue a écrit un notebook qui lit un gros
jeu de données CSV de transactions bancaires, puis produit un petit rapport quotidien (montant
total des transactions par pays pour une journée donnée).

Le notebook **fonctionne**, mais il est lent et coûteux. Votre mission est de rendre la **phase
de lecture** efficace — c'est là que se concentrent la majeure partie du temps et du coût.

Le code de départ se trouve dans [`starter_unoptimized.py`](./starter_unoptimized.py).

## Ce que fait le job

1. Lit un dossier CSV de transactions.
2. Filtre sur une date de transaction et un ensemble de pays.
3. Agrège le montant total des transactions par pays.
4. Affiche le résultat.

## Votre mission

Conservez un **résultat identique**, mais optimisez la façon dont les données sont lues et
traitées. Utilisez la Spark UI (onglets Jobs / Stages / SQL) pour mesurer avant et après.

### Pistes à explorer

- **Inférence du schéma.** Le lecteur parcourt tout le fichier juste pour deviner le type des
  colonnes. Que coûte `inferSchema=True` sur un gros fichier ? Quelles sont les alternatives ?
- **Format de fichier.** Le CSV est orienté ligne, non compressé, non « splittable » lorsqu'il
  est en gzip, et ne porte aucune statistique. Existe-t-il un meilleur format pour des lectures
  répétées ?
- **Lire plus que nécessaire.** Le job a besoin de 3 colonnes sur ~10 et d'1 journée parmi
  plusieurs. Quelle quantité de données est réellement parcourue ? Renseignez-vous sur le
  *column pruning* et le *predicate/partition pruning*.
- **Filtrer trop tard.** À quel endroit du plan le filtre intervient-il par rapport à la lecture ?
- **Travail répété.** Le même DataFrame est-il lu ou calculé plusieurs fois ?
- **Débogage avec `count()` / `collect()` / `print`.** Y a-t-il des actions qui déclenchent des
  jobs supplémentaires ?

### Objectifs d'optimisation (mesurables)

| Métrique                          | Où la voir                              |
|-----------------------------------|-----------------------------------------|
| Temps d'exécution (wall-clock)    | Durée de la cellule / job Spark UI      |
| Nombre de jobs & de stages        | Spark UI → Jobs                         |
| Octets / fichiers lus             | Spark UI → SQL → nœud de scan           |
| Lignes lues vs lignes nécessaires | Spark UI → SQL → « number of output rows » |

## Indices (à dévoiler progressivement)

<details>
<summary>Indice 1 — schéma</summary>

Ne payez pas le coût de `inferSchema`. Définissez un `StructType` explicite, ou lisez une fois
depuis un format qui stocke déjà le schéma (Parquet/Delta).
</details>

<details>
<summary>Indice 2 — ne lire que le nécessaire</summary>

Faites un `select` des 3 colonnes réellement utilisées, et un `filter` sur la date *avant*
d'agréger. Avec un format colonne, Spark peut élaguer les colonnes et ignorer des « row groups »
grâce aux statistiques de fichier.
</details>

<details>
<summary>Indice 3 — format & organisation</summary>

Convertissez la source une fois en Parquet ou Delta, partitionnée par `transaction_date`. Les
lectures suivantes ne touchent que la partition concernée (partition pruning) et les colonnes
concernées (column pruning).
</details>

<details>
<summary>Indice 4 — éviter les actions superflues</summary>

Chaque `count()`/`collect()`/`show()` est un job distinct. Retirez les actions de débogage du
chemin critique.
</details>

## Livrable

Un notebook optimisé qui produit le même rapport avec moins d'octets lus, moins de jobs et un
temps d'exécution plus faible. Soyez prêt à expliquer *quel* changement a apporté *quelle*
amélioration.

> Version anglaise : [`README.md`](./README.md)

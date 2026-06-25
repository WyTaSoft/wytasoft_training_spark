# UC1 — Lecture & agrégation simple

> 🇬🇧 English version: [`README.md`](./README.md)

## Contexte

Un job lit un dossier CSV de `transactions`, garde les lignes dont le `amount` est non vide, le
caste en `double`, somme le montant par `(client_id, transaction_type)`, et écrit le résultat en
Parquet.

Code de départ : [`pyspark/w001.py`](./pyspark/w001.py) (Scala : [`scala/w001.scala`](./scala/w001.scala)).

## Ce que fait le job

1. Lit toutes les colonnes du CSV en **chaînes** (sans schéma).
2. Filtre `amount != ""`, puis caste `amount` en double.
3. Agrège `sum(amount)` par `client_id`, `transaction_type`.
4. Écrit en Parquet.

## Votre mission

Conservez un **résultat identique**, mais rendez la lecture & la transformation moins coûteuses.
Utilisez la Spark UI (nœud de scan SQL) pour voir la quantité de données lues.

### Pistes à explorer

- **Pas de schéma.** Chaque colonne est lue en chaîne et `amount` est casté ensuite. Que vous
  apporte un `StructType` explicite (types corrects d'emblée, pas de passe de cast chaîne→double,
  et pas de scan `inferSchema` si vous l'activez un jour) ?
- **Lire plus que nécessaire.** Le job n'utilise que 3 des 5 colonnes. Renseignez-vous sur le
  *column pruning* (`select`).
- **Vide vs null.** `filter(amount != "")` puis cast — peut-on exprimer le filtre une seule fois,
  après une lecture typée (`amount IS NOT NULL`), et obtenir les mêmes lignes ?
- **Fichiers de sortie.** Le résultat est minuscule (une ligne par client/type). Combien de
  fichiers l'écriture produit-elle, et y a-t-il trop de petits fichiers ?

### Objectifs d'optimisation (mesurables)

| Métrique                       | Où la voir                            |
|--------------------------------|---------------------------------------|
| Colonnes lues                  | Spark UI → SQL → nœud de scan         |
| Nombre de stages / passes      | Spark UI → Jobs                       |
| Nombre de fichiers de sortie   | Le dossier Parquet cible              |
| Temps d'exécution              | Durée de la cellule / du job          |

## Indices

<details><summary>Indice 1 — schéma explicite</summary>

Déclarez un `StructType` avec `amount` en `double`. Pas de cast chaîne par ligne, types corrects
dès le départ.
</details>

<details><summary>Indice 2 — ne lire que le nécessaire</summary>

`select("client_id", "transaction_type", "amount")` avant d'agréger.
</details>

<details><summary>Indice 3 — un seul filtre</summary>

Avec une lecture typée, `filter(col("amount").isNotNull())` remplace le filtre chaîne vide + cast.
</details>

<details><summary>Indice 4 — éviter les petits fichiers</summary>

Le résultat agrégé est petit — `coalesce()` (ou partitionnez par `transaction_type`) pour ne pas
écrire des centaines de petits fichiers Parquet.
</details>

## Livrable

Un job optimisé produisant le même Parquet `(client_id, transaction_type, total_amount)`, lisant
moins de colonnes avec les bons types en une seule passe et un nombre de fichiers de sortie
raisonnable.

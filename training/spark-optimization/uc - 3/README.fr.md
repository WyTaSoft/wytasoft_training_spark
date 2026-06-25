# UC3 — Lecture & mise en cache d'un DataFrame réutilisé

> 🇬🇧 English version: [`README.md`](./README.md)

## Contexte

Un job lit un CSV `sales`, le filtre sur une liste de 10 produits prioritaires, et à partir de ce
**même DataFrame filtré** calcule deux rapports : le total des ventes par catégorie, et le top-5
des clients par ventes.

Code de départ : [`pyspark/w003.py`](./pyspark/w003.py) (Scala : [`scala/w003.scala`](./scala/w003.scala)).

## Ce que fait le job

1. Lit `sales` avec `inferSchema=true`.
2. Filtre `product_id IN (liste prioritaire)`.
3. À partir du DataFrame filtré : (a) somme par `category`, (b) top-5 clients par total des ventes.
4. `show()` des deux.

## Le problème

Le DataFrame filtré est utilisé par **deux** actions (`show()` sur chaque rapport). Comme il n'est
pas mis en cache, la lecture + le filtre s'exécutent **deux fois**. `inferSchema` ajoute encore une
passe complète.

## Votre mission

Conservez les deux rapports identiques, mais lisez et filtrez **une seule fois**. Utilisez la
Spark UI (Jobs + Storage).

### Pistes à explorer

- **`inferSchema`.** Il déclenche un scan supplémentaire juste pour deviner les types.
  Remplacez-le par un schéma explicite.
- **Column pruning.** Les rapports utilisent `product_id`, `category`, `client_id`, `amount` — pas
  `date`.
- **Mettre en cache le nœud réutilisé.** `filteredDf` alimente deux `show()`. Mettez-le en cache et
  matérialisez-le une fois pour que le second rapport soit servi depuis la mémoire.
- **Le cache est-il rentable ici ?** Seulement deux réutilisations — discutez du compromis face à
  l'acceptation de deux scans, et quand le cache l'emporte.

### Objectifs d'optimisation (mesurables)

| Métrique                            | Où la voir                            |
|-------------------------------------|---------------------------------------|
| Nombre de scans de la source / jobs | Spark UI → Jobs / nœuds de scan SQL   |
| Entrée en cache                     | Spark UI → onglet Storage             |
| Colonnes lues                       | Spark UI → nœud de scan SQL           |
| Temps d'exécution                   | Somme des durées de cellules          |

## Indices

<details><summary>Indice 1 — schéma explicite + pruning</summary>

Déclarez un `StructType`, et `select` uniquement les quatre colonnes utilisées par les rapports.
</details>

<details><summary>Indice 2 — cacher le DataFrame filtré</summary>

`filteredDf.cache()` puis une action (par ex. le premier rapport) remplit le cache ; le second
rapport lit depuis la mémoire.
</details>

<details><summary>Indice 3 — nettoyer</summary>

`unpersist()` après les deux rapports si vous continuez dans la même session.
</details>

## Livrable

Un job optimisé produisant les deux mêmes rapports, où la source est lue et filtrée une seule fois
(mise en cache), avec un schéma explicite et uniquement les colonnes nécessaires.

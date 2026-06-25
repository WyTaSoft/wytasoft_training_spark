# UC2 — Calcul répété & une jointure coûteuse

> 🇬🇧 English version: [`README.md`](./README.md)

## Contexte

Un job de détection de fraude lit une table `transactions` une fois, puis applique **trois règles**
pour signaler les clients suspects :

1. Retraits dont la somme glissante sur 7 jours dépasse 10 000 €.
2. Plus de 5 transactions étrangères (`country != FR`) sur une fenêtre glissante de 3 jours.
3. « Voyage impossible » : un déplacement de plus de 1000 km en moins de 2 heures.

Il fait l'union des identifiants clients suspects des trois règles et les écrit.

Code de départ : [`pyspark/w002.py`](./pyspark/w002.py) (Scala : [`scala/w002.scala`](./scala/w002.scala)).

## Les deux problèmes

- **Calcul répété.** Le `df` de base alimente la règle 1, la règle 2 **et** la règle 3. Rien n'est
  mis en cache : la lecture + le parsing repartent de zéro pour chaque règle (et encore pour
  l'écriture finale).
- **Une self-join coûteuse.** La règle 3 joint les transactions sur `client_id` avec un prédicat
  temporel pour comparer chaque paire de transactions — une explosion en `O(n²)` par client et un
  gros shuffle.

## Votre mission

Conservez un **ensemble de clients suspects équivalent**, mais arrêtez de recalculer la source et
remplacez la self-join par une approche qui passe à l'échelle. Utilisez la Spark UI
(Jobs + SQL + Stages).

### Pistes à explorer

- **Mettre en cache le nœud réutilisé.** `df` est lu une fois logiquement mais recalculé par
  action. Mettez en cache le DataFrame parsé et élagué, et matérialisez-le une fois.
- **Self-join vs window.** La règle 3 compare chaque transaction aux autres du même client dans un
  intervalle de 2 h. Une **fonction window** (`lag` sur `client_id` ordonné par `timestamp`)
  compare chaque transaction à la **précédente** — pas de self-join, un seul shuffle, linéaire en
  nombre de lignes. Discutez la différence sémantique (déplacements consécutifs vs toutes les
  paires) et pourquoi le consécutif est le signal réaliste de voyage impossible.
- **Column pruning.** Chaque règle n'a besoin que de quelques colonnes. Ne traînez pas toutes les
  colonnes à travers les shuffles.
- **Schéma explicite.** Évitez `inferSchema` (un scan complet supplémentaire).

### Objectifs d'optimisation (mesurables)

| Métrique                            | Où la voir                            |
|-------------------------------------|---------------------------------------|
| Nombre de scans de la source / jobs | Spark UI → Jobs / nœuds de scan SQL   |
| Entrée en cache                     | Spark UI → onglet Storage             |
| Octets de shuffle (règle 3)         | Spark UI → Stages                     |
| Temps d'exécution                   | Durée totale du job                   |

## Indices

<details><summary>Indice 1 — cacher la base</summary>

Après une lecture à schéma explicite et un `select` des colonnes utiles aux règles, `persist`-ez
et lancez une action pour que les trois règles lisent depuis le cache.
</details>

<details><summary>Indice 2 — supprimer la self-join</summary>

`Window.partitionBy("client_id").orderBy("timestamp")` + `lag(...)` pour obtenir la position et
l'heure précédentes. Calculez la distance haversine au point précédent et les secondes écoulées ;
signalez les lignes où l'écart < 7200 et la distance > 1000.
</details>

<details><summary>Indice 3 — unpersist</summary>

Libérez le cache une fois les suspects écrits.
</details>

## Livrable

Un job optimisé produisant l'ensemble équivalent de clients suspects, avec la source mise en cache
(lue une seule fois) et la règle 3 implémentée avec une window plutôt qu'une self-join. Soyez prêt
à montrer l'onglet Storage et la baisse de shuffle pour la règle 3.

# UC03 — Mise en cache & calcul répété

> 🇬🇧 English version: [`README.md`](./README.md)

## Contexte

Même banque. Un collègue construit un petit notebook de « KPIs quotidiens » à partir de la table
**transactions**. La transformation n'est pas triviale (parsing, nettoyage, dérivation de quelques
colonnes), et le **même DataFrame dérivé est réutilisé plusieurs fois** — pour un total, une
répartition par pays, une répartition par canal, et quelques vérifications `count()`.

Comme rien n'est mis en cache, Spark **recalcule tout le pipeline depuis les fichiers source à
chaque fois** qu'une action s'exécute. Le notebook **fonctionne** mais effectue bien plus de
travail que nécessaire.

Le code de départ se trouve dans [`starter_unoptimized.py`](./starter_unoptimized.py).

## Ce que fait le job

1. Lit `transactions`, la nettoie et dérive quelques colonnes (transformation modérément coûteuse).
2. Filtre sur les transactions « POSTED » d'un mois.
3. À partir du **même** DataFrame, calcule : un total général, un rapport par pays, un rapport par
   canal, et plusieurs vérifications `count()`.

## Votre mission

Conservez des **résultats identiques**, mais arrêtez de recalculer le pipeline partagé. Utilisez la
**Spark UI** (onglet Jobs + onglet Storage) pour compter combien de fois la source est lue avant
et après.

### Pistes à explorer

- **Évaluation paresseuse (lazy).** Les transformations sont lazy ; seules les **actions**
  déclenchent du travail. Chaque `count()` / `display()` / `collect()` ré-exécute le lignage
  depuis la source tant que rien n'est matérialisé. Combien d'actions le code de départ lance-t-il,
  et combien de scans complets cela provoque-t-il ?
- **`cache()` / `persist()`.** Que fait exactement la mise en cache, et *quand* (c'est lazy aussi :
  la première action remplit le cache). Quel `StorageLevel` a du sens ici ?
- **Où mettre en cache.** Mettez en cache le nœud *réutilisé* — après la transformation/le filtre
  coûteux, pas la lecture brute. Mettre en cache trop tôt, ou cacher quelque chose utilisé une
  seule fois, gaspille de la mémoire.
- **Libérer la mémoire.** Que se passe-t-il si on cache sans jamais faire `unpersist()` ? Quand
  faut-il libérer ?
- **Alternatives au cache.** Parfois la bonne réponse n'est *pas* le cache : une seule agrégation
  combinée, ou l'écriture d'une table Delta intermédiaire pour réutilisation en aval. Quand
  privilégier l'un ou l'autre ?
- **Ne pas compter pour déboguer.** Chaque `count()` est un job complet. Ces vérifications
  valent-elles leur coût ?

### Objectifs d'optimisation (mesurables)

| Métrique                              | Où la voir                                   |
|---------------------------------------|----------------------------------------------|
| Nombre de jobs / scans de la source   | Spark UI → Jobs (et nœuds de scan SQL)       |
| Octets lus (somme sur les jobs)       | Spark UI → SQL → nœuds de scan               |
| Taille / fraction en cache            | Spark UI → onglet **Storage**                |
| Temps d'exécution (tout le notebook)  | Somme des durées de cellules                 |

## Indices (à dévoiler progressivement)

<details>
<summary>Indice 1 — trouver le DataFrame réutilisé</summary>

Identifiez le nœud d'où partent tous les rapports en aval (les transactions nettoyées + filtrées).
Ce DataFrame unique est recalculé une fois par action. C'est le candidat à la mise en cache.
</details>

<details>
<summary>Indice 2 — le cacher, puis matérialiser une fois</summary>

Appelez `.cache()` (ou `.persist(StorageLevel.MEMORY_AND_DISK)`) sur ce DataFrame, puis déclenchez
**une** action pour remplir le cache. Les rapports suivants lisent depuis la mémoire, pas depuis la
source. Vérifiez dans l'onglet **Storage** que c'est bien en cache.
</details>

<details>
<summary>Indice 3 — supprimer les count() de debug (ou les faire une fois)</summary>

Chaque `count()` est un job complet distinct. Retirez-les du chemin critique, ou calculez toutes
les métriques de vérification en une seule passe avec une seule agrégation.
</details>

<details>
<summary>Indice 4 — nettoyer, et envisager de ne pas cacher du tout</summary>

Faites `unpersist()` quand vous avez terminé pour ne pas immobiliser la mémoire. Si les données
dérivées sont réutilisées entre *notebooks/jobs* (pas seulement dans celui-ci), écrire une table
**Delta** intermédiaire est souvent préférable à un cache en mémoire. Si elles ne servent qu'une
fois, ne les cachez pas.
</details>

## Livrable

Un notebook optimisé produisant les mêmes rapports total / par pays / par canal, où le pipeline
partagé est calculé **une seule fois** (caché ou matérialisé), avec une source lue bien moins
souvent. Soyez prêt à montrer la baisse du nombre de jobs/scans dans la Spark UI et l'entrée en
cache dans l'onglet Storage.

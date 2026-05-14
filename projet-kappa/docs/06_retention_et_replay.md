# 🔄 Rétention et Replay — Le Cœur de l'Architecture Kappa

## 1. Pourquoi la Rétention est Essentielle

La **rétention des événements** est le concept qui fait de Kappa une architecture viable. Sans rétention, impossible de faire du replay. Sans replay, pas de Kappa.

### Définition
> La **rétention** est la durée pendant laquelle Kafka conserve les événements dans ses topics. Après cette durée, les événements sont automatiquement supprimés.

### Dans notre projet

| Topic | Rétention | Justification |
|-------|-----------|---------------|
| `clics_ecommerce` | **7 jours** | Permet de rejouer une semaine complète d'événements |
| `clics_filtres` | **3 jours** | Données nettoyées, moins besoin d'historique long |
| `stats_produits` | **1 jour** | Résultats de calcul, recalculables par replay |
| `alertes_marketing` | **7 jours** | Suivi des alertes par l'équipe marketing |

### Configuration technique

```bash
# Créer un topic avec 7 jours de rétention (604800000 ms)
kafka-topics --create \
  --topic clics_ecommerce \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --config retention.ms=604800000

# Vérifier la configuration
kafka-topics --describe \
  --topic clics_ecommerce \
  --bootstrap-server localhost:9092
```

## 2. Le Replay — Alternative au Batch Layer

### Pourquoi rejouer ?

Dans l'architecture Lambda, quand on veut recalculer des résultats (ex: correction d'un bug dans la formule de conversion), il faut relancer le **batch layer** (MapReduce/Spark) sur tout le dataset.

Dans l'architecture Kappa, on fait simplement un **replay** :
1. On déploie la nouvelle version de la topologie
2. On configure le consommateur pour lire depuis le **début** du log
3. Les résultats sont recalculés automatiquement

### Comment ça fonctionne techniquement ?

```python
# TEMPS RÉEL : lire les nouveaux événements uniquement
consumer = KafkaConsumer(
    'clics_ecommerce',
    auto_offset_reset='latest',    # ← Seulement les nouveaux
    group_id='traitement-v1'
)

# REPLAY : relire TOUT depuis le début
consumer = KafkaConsumer(
    'clics_ecommerce',
    auto_offset_reset='earliest',  # ← Depuis le début
    group_id=None                  # ← Pas de groupe (lecture indépendante)
)
```

Le paramètre clé est `auto_offset_reset='earliest'` qui force Kafka à renvoyer les événements **depuis le tout début** du log.

### Illustration du Replay

```
  LOG KAFKA (clics_ecommerce)
  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐
  │ e1 │ e2 │ e3 │ e4 │ e5 │ e6 │ e7 │ e8 │ e9 │e10 │
  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘
   ▲                                              ▲
   │                                              │
   └── REPLAY : commence ici (earliest)           │
                                                  │
                           TEMPS RÉEL : lit ici (latest)

  Le replay retraite TOUS les événements e1→e10
  Le temps réel ne traite que les NOUVEAUX (e11, e12, ...)
```

## 3. Validation de Cohérence

### Le Test Fondamental

Pour prouver que l'architecture Kappa fonctionne, on doit montrer que :

> **Replay == Temps Réel**

Les résultats obtenus en rejouant le log depuis le début doivent être **identiques** à ceux calculés en temps réel.

### Notre script de test (`config/test_replay.py`)

Le script exécute les étapes suivantes :

```
 Étape 1: Lire TOUS les événements bruts (clics_ecommerce)
   → ex: 500 événements depuis le début
   
 Étape 2: Rejouer le filtrage
   → ex: 400 humains, 100 bots
   
 Étape 3: Rejouer l'agrégation
   → Recalculer les stats par produit
   
 Étape 4: Comparer avec les résultats temps réel
   → Lire le topic clics_filtres (rempli par filtrage.py en temps réel)
   → Calculer les mêmes stats
   → Comparer produit par produit

 Étape 5: Conclure
   → Si identique : ✅ Le système est cohérent
   → Si différent : ❌ Incohérence détectée
```

### Résultat attendu

```
  🔍 VALIDATION DE COHÉRENCE
  ──────────────────────────────────────────────────────────
  ✅ PC_Gamer          | vues            | Replay: 45 | RT: 45
  ✅ PC_Gamer          | achats          | Replay: 12 | RT: 12
  ✅ Souris_Sans_Fil   | vues            | Replay: 38 | RT: 38
  ✅ Souris_Sans_Fil   | achats          | Replay:  8 | RT:  8
  ...
  ──────────────────────────────────────────────────────────
  ✅ RÉSULTAT : Le système est COHÉRENT — Replay = Temps Réel
```

## 4. Scénarios de Replay en Entreprise

| Scénario | Action | Bénéfice |
|----------|--------|----------|
| **Bug dans la formule** | Corriger le code, rejouer | Résultats corrigés sans perte |
| **Nouveau KPI demandé** | Ajouter la métrique, rejouer | KPI historique disponible immédiatement |
| **Audit de données** | Rejouer et comparer | Preuve de cohérence |
| **Migration de système** | Rejouer sur le nouveau système | Validation de la migration |
| **Test de charge** | Rejouer N fois plus vite | Mesure de performance |

## 5. Limites de la Rétention

| Limite | Impact | Solution |
|--------|--------|----------|
| Espace disque | Plus de rétention = plus de stockage | Compression des messages |
| Temps de replay | Rejouer 7 jours prend du temps | Replay partiel (depuis un offset) |
| Coût | Stockage cloud coûteux | Politique de rétention adaptée |

> **Bonne pratique** : Définir la rétention en fonction du besoin métier. 7 jours suffisent pour notre cas d'usage e-commerce, mais un système bancaire pourrait nécessiter 90 jours ou plus.

# 📐 Architecture Kappa — Fondements Théoriques

## 1. Qu'est-ce que l'Architecture Kappa ?

L'architecture Kappa est une architecture de traitement de données proposée par **Jay Kreps** (co-créateur d'Apache Kafka) en 2014, comme alternative simplifiée à l'architecture Lambda.

### Principe fondamental :
> **Un unique flux de traitement continu** pour toutes les données, qu'elles soient en temps réel ou historiques.

Contrairement à Lambda qui sépare le traitement en deux couches (batch + speed), Kappa repose sur l'idée que si le système de streaming est suffisamment robuste, il n'y a pas besoin d'un batch layer séparé.

## 2. Architecture Lambda vs Kappa

### Architecture Lambda (2011 — Nathan Marz)

```
                    ┌──────────────────┐
                    │   BATCH LAYER    │ ← Retraitement périodique
                    │  (MapReduce,     │    (toutes les heures/jours)
     Données ──────►│   Spark Batch)   │──────┐
     brutes         └──────────────────┘      │
        │                                      ▼
        │           ┌──────────────────┐  ┌──────────┐
        └──────────►│   SPEED LAYER    │──►│ SERVING  │──► Résultats
                    │  (Storm, Flink)  │  │  LAYER   │
                    │  Temps réel      │  └──────────┘
                    └──────────────────┘
```

**Problèmes de Lambda :**
- **Duplication de code** : la même logique métier doit être écrite 2 fois (batch + stream)
- **Complexité opérationnelle** : maintenir 2 systèmes distincts
- **Résultats potentiellement incohérents** entre les 2 couches

### Architecture Kappa (2014 — Jay Kreps)

```
                    ┌──────────────────────────────────┐
     Données ──────►│      STREAM PROCESSING LAYER     │──► Résultats
     brutes         │  (Kafka + Flink/Processing)      │
                    │  Un seul flux pour TOUT           │
                    └──────────────────────────────────┘
                              │
                              │ REPLAY si besoin
                              │ (relire depuis le début)
                              ▼
                    ┌──────────────────────────────────┐
                    │      LOG D'ÉVÉNEMENTS (Kafka)    │
                    │  Rétention configurée (7 jours)  │
                    │  Immuable + Ordonné               │
                    └──────────────────────────────────┘
```

**Avantages de Kappa :**
- **Simplicité** : une seule codebase pour le traitement
- **Cohérence** : un seul résultat, pas de merge entre batch et speed
- **Replay** : pour recalculer, on rejoue le flux depuis le début
- **Moins de maintenance** : moins de systèmes à gérer

## 3. Les 3 Piliers de Kappa

### Pilier 1 : Le Log d'Événements (Kafka)
- Tous les événements sont stockés de manière **immuable** et **ordonnée**
- La **rétention** définit combien de temps on garde les événements
- Permet le **replay** : relire l'historique depuis n'importe quel point

### Pilier 2 : Le Traitement de Flux (Topologies)
- Les événements sont traités **en continu** par des topologies
- Chaque topologie a une responsabilité unique (filtrage, agrégation, détection)
- Les topologies forment une **chaîne de traitement** (pipeline)

### Pilier 3 : Le Replay pour la Cohérence
- Si on change la logique de traitement → on déploie une nouvelle version
- La nouvelle version **rejoue** tous les événements depuis le début
- Le résultat est **identique** à ce qu'aurait produit un batch layer

## 4. Pourquoi Kappa pour notre projet E-commerce ?

| Critère | Lambda | Kappa | Notre choix |
|---------|--------|-------|-------------|
| Temps réel | ✅ | ✅ | Kappa |
| Simplicité | ❌ (2 systèmes) | ✅ (1 système) | Kappa |
| Cohérence | ⚠️ (merge complexe) | ✅ (replay) | Kappa |
| Replay | ❌ (batch séparé) | ✅ (natif Kafka) | Kappa |
| Coût opérationnel | Élevé | Réduit | Kappa |

Pour une plateforme e-commerce, les événements comportementaux (clics, paniers, achats) sont **naturellement des flux**. L'architecture Kappa est parfaitement adaptée car :
1. Les données arrivent **en continu** (les clients naviguent en permanence)
2. On a besoin de **réactivité** (détecter un abandon de panier immédiatement)
3. On veut pouvoir **recalculer** les statistiques si la logique change

## 5. Notre Architecture Kappa — Vue d'ensemble

```
  ┌─────────────────┐
  │   🏪 SIMULATEUR  │  Génère les événements e-commerce
  │   (simulateur.py)│  (vues, paniers, achats, abandons)
  └────────┬────────┘
           │ produce
           ▼
  ┌─────────────────┐     ┌──────────────────┐
  │  📨 KAFKA       │────►│  🔍 FILTRAGE     │  Topologie 1
  │  clics_ecommerce│     │  (filtrage.py)    │  Élimine les bots
  │  Rétention: 7j  │     └────────┬─────────┘
  └─────────────────┘              │
                                   ▼
                          ┌─────────────────┐
                          │  📨 KAFKA       │
                          │  clics_filtres   │
                          │  Rétention: 3j  │
                          └───────┬─┬───────┘
                                  │ │
                    ┌─────────────┘ └─────────────┐
                    ▼                             ▼
          ┌──────────────────┐          ┌──────────────────┐
          │  📊 AGRÉGATION   │          │  🚨 DÉTECTION    │
          │  (agregation.py)  │          │  ABANDON         │
          │  Topologie 2     │          │  (detection.py)   │
          │  Stats par produit│          │  Topologie 3     │
          └────────┬─────────┘          └────────┬─────────┘
                   │                             │
                   ▼                             ▼
          ┌─────────────────┐          ┌─────────────────┐
          │  📨 KAFKA       │          │  📨 KAFKA       │
          │  stats_produits  │          │  alertes_        │
          │                 │          │  marketing       │
          └─────────────────┘          └─────────────────┘
```

## 6. Technologies Utilisées

| Technologie | Rôle | Version |
|-------------|------|---------|
| **Apache Kafka** | Log d'événements, rétention, distribution | 7.3.0 (Confluent) |
| **Apache Zookeeper** | Coordination du cluster Kafka | 7.3.0 (Confluent) |
| **Apache Flink** | Moteur de traitement de flux (référence) | 1.17 |
| **Python** | Scripts de simulation et traitement | 3.10+ |
| **kafka-python** | Client Python pour Kafka | 2.3.x |
| **Docker** | Conteneurisation de l'infrastructure | 29.x |
| **Docker Compose** | Orchestration des services | 5.x |

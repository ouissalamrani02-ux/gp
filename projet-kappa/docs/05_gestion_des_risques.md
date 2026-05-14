# ⚠️ Gestion des Risques

## 1. Introduction

Conformément au **Chapitre 9** du cours (Les Risques — Pr. Abakouy), nous avons identifié, priorisé et défini des actions de prévention pour les risques de ce projet.

La gestion des risques comprend 4 étapes :
1. **Identifier** les risques potentiels
2. **Prioriser** par probabilité × impact
3. **Prévenir** ou réduire les risques
4. **Suivre** les risques tout au long du projet

## 2. Matrice des Risques

### 2.1. Risques Techniques

| # | Risque | Probabilité | Impact | Criticité | Statut |
|---|--------|:-----------:|:------:|:---------:|--------|
| R1 | **Perte de données Kafka** — Le broker Kafka tombe et les événements sont perdus | Moyenne | Élevé | 🔴 Critique | Mitigé |
| R2 | **Latence du traitement** — Les topologies ne traitent pas assez vite | Faible | Moyen | 🟡 Modéré | Mitigé |
| R3 | **Incompatibilité des versions** — Docker/Kafka/Flink incompatibles | Moyenne | Élevé | 🔴 Critique | Résolu |
| R4 | **Mémoire insuffisante** — Docker consomme trop de RAM | Moyenne | Moyen | 🟡 Modéré | Mitigé |
| R5 | **Incohérence des données** — Replay ≠ temps réel | Faible | Élevé | 🟡 Modéré | Résolu |

### 2.2. Risques de Gestion de Projet

| # | Risque | Probabilité | Impact | Criticité | Statut |
|---|--------|:-----------:|:------:|:---------:|--------|
| R6 | **Retard de livraison** — Complexité sous-estimée | Élevée | Élevé | 🔴 Critique | Mitigé |
| R7 | **Mauvaise répartition des tâches** — Un membre surchargé | Moyenne | Moyen | 🟡 Modéré | Mitigé |
| R8 | **Manque de compétences** — Technologies nouvelles (Kafka, Flink) | Élevée | Moyen | 🟡 Modéré | Résolu |
| R9 | **Communication défaillante** — Malentendus MOA/MOE | Faible | Moyen | 🟢 Faible | Mitigé |

### 2.3. Visualisation de la Matrice

```
 Impact ▲
 Élevé  │  R8     R1,R3,R6
        │
 Moyen  │  R9     R4,R7    R2
        │
 Faible │
        └──────────────────────► Probabilité
           Faible  Moyenne  Élevée
```

## 3. Plan de Mitigation

### R1 : Perte de données Kafka
| Aspect | Détail |
|--------|--------|
| **Cause** | Le serveur Kafka crash ou le disque est plein |
| **Conséquence** | Perte d'événements clients, statistiques faussées |
| **Action préventive** | Configuration de la rétention (7 jours) |
| **Action corrective** | Replay depuis le dernier offset connu |
| **Responsable** | MOE (Développeur infrastructure) |

> Dans une vraie production, on configurerait la **réplication** (replication-factor > 1) et un cluster multi-brokers. Pour notre démo, nous utilisons un seul broker.

### R2 : Latence du traitement
| Aspect | Détail |
|--------|--------|
| **Cause** | Trop d'événements par seconde, topologies trop lentes |
| **Conséquence** | Alertes d'abandon en retard, stats obsolètes |
| **Action préventive** | Partitionnement des topics (3 partitions) |
| **Action corrective** | Augmenter les task slots Flink |
| **Responsable** | MOE (Développeur traitement) |

### R3 : Incompatibilité des versions
| Aspect | Détail |
|--------|--------|
| **Cause** | Versions de Docker, Kafka, Flink non compatibles |
| **Conséquence** | L'infrastructure ne démarre pas |
| **Action préventive** | Figer les versions dans docker-compose (cp-kafka:7.3.0, flink:1.17) |
| **Action corrective** | Tester avec des versions antérieures |
| **Statut** | ✅ Résolu par le choix de versions stables |

### R6 : Retard de livraison
| Aspect | Détail |
|--------|--------|
| **Cause** | Sous-estimation de la complexité technique |
| **Conséquence** | Projet incomplet à la date de soutenance |
| **Action préventive** | Estimation par **Planning Poker** (cf. cours Ch.8) |
| **Action corrective** | Réduire le périmètre (prioriser les fonctionnalités critiques) |
| **Responsable** | Chef de Projet |

## 4. Le Cône d'Incertitude

Référence au cours (Chapitre 8 — Estimation de charge) :

```
 Incertitude ▲
     4x      │╲
             │ ╲
     2x      │  ╲
             │   ╲──────────────────
     1x      │                      ╲──────
             │
     0.5x    │
             └─────────────────────────────► Temps
             Début    Sprint1   Sprint2   Fin
```

- **Au début du projet** : incertitude × 4 (on ne connaît pas bien Kafka/Flink)
- **Après Sprint 1** : incertitude × 2 (l'infrastructure fonctionne)
- **Après Sprint 2** : incertitude × 1 (les topologies sont validées)

## 5. Planning Poker — Estimation des Charges

Nous avons utilisé la méthode du **Planning Poker** pour estimer les story points :

| User Story | Estimation initiale | Estimation consensuelle | Réel |
|-----------|:---:|:---:|:---:|
| US1 : Déployer Kafka | 3 | 5 | 5 |
| US2 : Simulateur | 2 | 3 | 3 |
| US3 : Filtrage | 3 | 3 | 3 |
| US4 : Agrégation | 5 | 5 | 5 |
| US5 : Détection abandon | 5 | 8 | 5 |
| US6 : Replay | 3 | 3 | 3 |

**Leçon apprise** : L'estimation initiale sous-estimait souvent la complexité. Le Planning Poker permet d'avoir des estimations plus réalistes grâce au consensus de l'équipe.

## 6. Suivi des Risques

| Risque | Sprint 1 | Sprint 2 | Sprint 3 |
|--------|:---:|:---:|:---:|
| R1 Perte données | ⚠️ Non traité | ✅ Rétention config | ✅ Validé |
| R2 Latence | 🟢 Pas de problème | 🟢 Acceptable | ✅ OK |
| R3 Versions | ✅ Résolu | ✅ | ✅ |
| R4 Mémoire | ⚠️ Docker lourd | ✅ Optimisé | ✅ OK |
| R6 Retard | ⚠️ Sprint 1 long | 🟢 Rattrapage | ✅ Livré |

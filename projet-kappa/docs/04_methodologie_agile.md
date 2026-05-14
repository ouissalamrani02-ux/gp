# 🔄 Méthodologie Agile — Scrum & Kanban

## 1. Choix de la Méthodologie

Conformément au **Chapitre 4** du cours (Cycles de vie — Pr. Abakouy), nous avons adopté une approche **Agile (Scrum + Kanban)** pour ce projet.

### Pourquoi Agile ?

| Critère | Cascade (Waterfall) | Agile (Scrum) | Notre choix |
|---------|:---:|:---:|:---:|
| Besoins évolutifs | ❌ Figés au début | ✅ Adaptatifs | **Agile** |
| Livraison | Unique à la fin | Incrémentale | **Agile** |
| Feedback | Tardif | Continu | **Agile** |
| Risques | Détectés tard | Détectés tôt | **Agile** |
| Documentation | Lourde | Légère et utile | **Agile** |

Notre projet est un **projet d'innovation** (niveau d'incertitude élevé) avec des technologies nouvelles (Kafka, Flink). L'approche Agile nous permet d'avancer par itérations et de valider progressivement.

## 2. Organisation en Sprints

### Sprint 0 : Cadrage (01/04 — 02/04/2026)
**Objectif** : Définir le périmètre et comprendre l'architecture Kappa

| Tâche | Livrable | Statut |
|-------|----------|--------|
| Cadrage du besoin métier (MOA) | Cahier des charges informel | ✅ |
| Choix de l'architecture (Kappa vs Lambda) | Document de décision | ✅ |
| Mise en place de Taiga (Kanban) | Board configuré | ✅ |
| Planification (Gantt) | Diagramme de Gantt | ✅ |

### Sprint 1 : Infrastructure (06/04 — 08/04/2026)
**Objectif** : Déployer l'infrastructure Docker (Kafka + Flink)

| Tâche | Livrable | Statut |
|-------|----------|--------|
| Rédiger le docker-compose.yml | Fichier de configuration | ✅ |
| Lancer Zookeeper + Kafka | Conteneurs opérationnels | ✅ |
| Lancer Flink (JobManager + TaskManager) | Interface Flink accessible | ✅ |
| Créer le topic `clics_ecommerce` | Topic Kafka fonctionnel | ✅ |
| Test : produire/consommer un message | Preuve de fonctionnement | ✅ |

### Sprint 2 : Simulateur + Topologies (09/04 — 13/04/2026)
**Objectif** : Développer le simulateur et les topologies de traitement

| Tâche | Livrable | Statut |
|-------|----------|--------|
| Développer simulateur.py (v1) | Script de simulation basique | ✅ |
| Améliorer simulateur.py (v2 : bots, prix, sessions) | Simulateur complet | ✅ |
| Développer la topologie de filtrage | `filtrage.py` fonctionnel | ✅ |
| Développer la topologie d'agrégation | `agregation.py` fonctionnel | ✅ |
| Développer la détection d'abandon | `detection_abandon.py` fonctionnel | ✅ |
| Configurer les topics avec rétention | `setup_topics.sh` | ✅ |

### Sprint 3 : Validation + Rapport (14/04 — 16/04/2026)
**Objectif** : Valider la cohérence et rédiger le dossier

| Tâche | Livrable | Statut |
|-------|----------|--------|
| Test de replay | `test_replay.py` | ✅ |
| Validation de cohérence | Résultats documentés | ✅ |
| Rédaction du rapport final | Rapport complet | ✅ |
| Préparation de la présentation | Slides | 🔶 En cours |

## 3. Kanban Board (Taiga)

Notre board Kanban dans Taiga est organisé en 5 colonnes :

```
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│  NEW     │  │  READY   │  │IN PROGR. │  │READY FOR │  │  DONE    │
│          │  │          │  │          │  │  TEST    │  │          │
│          │  │ #5 US    │  │ #3 Script│  │          │  │ #1 Docker│
│          │  │ Marketing│  │ Simul.   │  │          │  │ compose  │
│          │  │          │  │          │  │          │  │          │
│          │  │ #6 Rédiger│  │ #4 Port │  │          │  │ #2 Lancer│
│          │  │ Dossier  │  │ Kafka    │  │          │  │ conteneur│
└──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘
```

### User Stories

| # | User Story | Priorité | Sprint |
|---|-----------|----------|--------|
| US1 | En tant que MOE, je veux déployer Kafka pour recevoir les événements | Haute | Sprint 1 |
| US2 | En tant que MOE, je veux un simulateur pour générer des données test | Haute | Sprint 2 |
| US3 | En tant que MOA, je veux filtrer les bots pour avoir des stats fiables | Haute | Sprint 2 |
| US4 | En tant que MOA, je veux voir les stats par produit en temps réel | Haute | Sprint 2 |
| US5 | En tant que MOA, je veux être alerté des abandons de panier | Haute | Sprint 2 |
| US6 | En tant que MOE, je veux valider la cohérence par replay | Moyenne | Sprint 3 |

## 4. Vélocité de l'Équipe

| Sprint | Story Points prévus | Story Points réalisés | Vélocité |
|--------|:---:|:---:|:---:|
| Sprint 1 | 8 | 8 | 100% |
| Sprint 2 | 13 | 13 | 100% |
| Sprint 3 | 8 | 8 | 100% |

## 5. Validation (Recette)

À chaque fin de sprint, la MOA (Marketing) vérifie que :

1. **Sprint 1** : Les conteneurs Docker sont opérationnels ✅
2. **Sprint 2** : Le simulateur envoie des données, les topologies les traitent ✅
3. **Sprint 3** : Le replay produit des résultats cohérents ✅

C'est le processus de **recette** (Chapitre 3 — Étapes projet).

## 6. Outils Agile Utilisés

| Outil | Rôle | Référence cours |
|-------|------|-----------------|
| **Taiga** | Kanban board, suivi des US | Ch.5 — Suivi du projet |
| **GanttProject** | Planification temporelle | Ch.7 — Planification |
| **Git** | Versioning du code | Bonne pratique DevOps |
| **Docker** | Déploiement reproductible | Sprint 1 |

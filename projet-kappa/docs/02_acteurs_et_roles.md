# 👥 Acteurs et Rôles — Gouvernance du Projet

## 1. Contexte

Dans le cadre de ce projet de **Gestion de Projets Informatiques** (Pr. Abakouy — ENSA Al Hoceima), nous identifions les acteurs selon le modèle **MOA / MOE / Chef de Projet** enseigné en cours (Chapitre 2).

## 2. Les Parties Prenantes

### 2.1. Maîtrise d'Ouvrage (MOA) — Le Client

> **« Que faut-il faire ? »**

Dans notre cas d'usage e-commerce, la MOA est l'**équipe Marketing** de la plateforme fictive.

| Aspect | Détail |
|--------|--------|
| **Qui ?** | Directeur Marketing de la plateforme e-commerce |
| **Besoin principal** | Comprendre le comportement des clients en temps réel |
| **Objectifs** | Réduire le taux d'abandon de panier, personnaliser l'expérience |
| **Exigences fonctionnelles** | - Voir les clics en temps réel |
| | - Recevoir des alertes quand un client abandonne son panier |
| | - Avoir des statistiques de vente par produit |
| **Validation** | Vérifier que les données affichées correspondent à la réalité |

### 2.2. Maîtrise d'Œuvre (MOE) — L'Équipe Technique

> **« Comment le faire ? »**

La MOE est notre **équipe de développement** qui implémente l'architecture Kappa.

| Aspect | Détail |
|--------|--------|
| **Qui ?** | L'équipe d'étudiants développeurs |
| **Rôle** | Concevoir et réaliser l'architecture technique |
| **Responsabilités** | - Déployer l'infrastructure (Docker, Kafka, Flink) |
| | - Développer les topologies de traitement |
| | - Configurer la rétention des événements |
| | - Valider la cohérence du système |
| **Contraintes techniques** | Technologies imposées : Kafka, Flink, Docker |

### 2.3. Le Chef de Projet

> **Le lien entre la MOA et la MOE**

| Aspect | Détail |
|--------|--------|
| **Rôle** | Coordonner la MOA (Marketing) et la MOE (Dev) |
| **Missions** | - Planifier les sprints (Gantt, Taiga) |
| | - Gérer les risques (retards, complexité technique) |
| | - S'assurer que la MOE ne construit pas une « usine à gaz » |
| | - Vérifier que la MOA comprend les contraintes techniques |
| **Outils** | Taiga (Kanban), GanttProject, Docker Desktop |

## 3. Organigramme du Projet

```
          ┌─────────────────────────────────┐
          │        COMITÉ DE PILOTAGE       │
          │   (Professeur — Pr. Abakouy)    │
          │   Valide les livrables finaux   │
          └───────────────┬─────────────────┘
                          │
          ┌───────────────┼─────────────────┐
          │               │                 │
    ┌─────▼─────┐  ┌──────▼──────┐  ┌──────▼──────┐
    │   MOA     │  │ CHEF DE     │  │    MOE      │
    │ Marketing │◄─┤   PROJET    ├─►│  Technique  │
    │           │  │             │  │             │
    │ - Besoins │  │ - Planning  │  │ - Kafka     │
    │ - Valide  │  │ - Risques   │  │ - Flink     │
    │ - Recette │  │ - Suivi     │  │ - Python    │
    └───────────┘  └─────────────┘  └─────────────┘
```

## 4. Matrice RACI

| Activité | MOA (Marketing) | Chef de Projet | MOE (Dev) |
|----------|:---:|:---:|:---:|
| Définir les besoins | **R** | C | I |
| Choisir l'architecture (Kappa) | C | **A** | **R** |
| Planifier les sprints | I | **R** | C |
| Déployer l'infrastructure | I | I | **R** |
| Développer les topologies | I | C | **R** |
| Configurer la rétention | I | I | **R** |
| Tester la cohérence | C | **A** | **R** |
| Valider les résultats (recette) | **R** | A | I |
| Rédiger le rapport | C | **R** | C |
| Présenter le projet | **R** | **R** | **R** |

*R = Responsable, A = Approbateur, C = Consulté, I = Informé*

## 5. Communication entre Acteurs

### 5.1. Réunions
- **Daily standup** (informel) : point quotidien entre les membres
- **Sprint review** : à chaque fin de sprint, démonstration à la MOA
- **Sprint retrospective** : retour d'expérience après chaque sprint

### 5.2. Outils de Communication
| Outil | Usage |
|-------|-------|
| **Taiga** | Kanban board — suivi des tâches |
| **GanttProject** | Planification temporelle |
| **WhatsApp/Discord** | Communication instantanée |
| **Git** | Gestion du code source |

## 6. Facteurs Clés de Succès

1. **Définition claire des rôles** — Chacun sait ce qu'il doit faire
2. **Communication régulière** — Éviter les malentendus MOA/MOE
3. **Validation progressive** — Ne pas attendre la fin pour tester
4. **Documentation** — Tout est tracé et justifié

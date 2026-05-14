# 🏗️ Architecture Kappa — Traitement Événementiel Unifié

## Plateforme E-commerce Fictive

> **Projet de Gestion de Projets Informatiques**  
> Pr. Redouan ABAKOUY — ENSA Al Hoceima  
> Année universitaire 2025-2026

---

## 📋 Description

Ce projet implémente l'**architecture Kappa**, une alternative unifiée à Lambda, reposant sur un unique flux de traitement continu. Le cas d'usage est un flux d'événements comportementaux sur une plateforme e-commerce fictive.

### Architecture

```
 Simulateur → Kafka → Filtrage → Agrégation → Stats
                              └→ Détection abandon → Alertes Marketing
```

## 🛠️ Stack Technique

| Technologie | Rôle |
|-------------|------|
| Apache Kafka | Log d'événements, rétention |
| Apache Flink | Moteur de traitement (référence) |
| Python | Simulation et topologies |
| Docker | Conteneurisation |

## 📁 Structure du Projet

```
projet-kappa/
├── docker-compose.yml              # Infrastructure (Kafka, Flink, Zookeeper)
├── simulateur.py                   # Simulateur d'événements e-commerce
├── stream_processing/              # Topologies de traitement
│   ├── filtrage.py                 # Topologie 1 : Filtrage des bots
│   ├── agregation.py               # Topologie 2 : Agrégation temps réel
│   └── detection_abandon.py        # Topologie 3 : Détection d'abandon
├── config/                         # Configuration
│   ├── setup_topics.sh             # Création des topics Kafka
│   └── test_replay.py              # Test de replay et cohérence
├── docs/                           # Documentation
│   ├── 01_architecture_kappa.md    # Théorie Kappa vs Lambda
│   ├── 02_acteurs_et_roles.md      # MOA, MOE, Chef de Projet
│   ├── 03_implementation_technique.md # Guide technique détaillé
│   ├── 04_methodologie_agile.md    # Sprints, Kanban, User Stories
│   ├── 05_gestion_des_risques.md   # Matrice des risques
│   ├── 06_retention_et_replay.md   # Rétention et replay
│   └── 07_guide_execution.md       # Guide de démonstration
└── rapport/
    └── rapport_final.md            # Rapport de projet complet
```

## 🚀 Démarrage Rapide

```bash
# 1. Démarrer l'infrastructure
docker-compose up -d

# 2. Créer les topics Kafka
docker exec -it projet-kappa-kafka-1 kafka-topics --create \
  --topic clics_ecommerce --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it projet-kappa-kafka-1 kafka-topics --create \
  --topic clics_filtres --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

docker exec -it projet-kappa-kafka-1 kafka-topics --create \
  --topic alertes_marketing --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

docker exec -it projet-kappa-kafka-1 kafka-topics --create \
  --topic stats_produits --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

# 3. Installer les dépendances Python
pip install kafka-python

# 4. Lancer le simulateur (Terminal 1)
python simulateur.py

# 5. Lancer les topologies (Terminaux 2, 3, 4)
python stream_processing/filtrage.py
python stream_processing/agregation.py
python stream_processing/detection_abandon.py

# 6. Test de replay
python config/test_replay.py

# 7. Arrêter
docker-compose down
```

## 📊 Topics Kafka

| Topic | Contenu | Rétention |
|-------|---------|-----------|
| `clics_ecommerce` | Événements bruts | 7 jours |
| `clics_filtres` | Événements sans bots | 3 jours |
| `stats_produits` | Statistiques agrégées | 1 jour |
| `alertes_marketing` | Alertes d'abandon | 7 jours |

## 📖 Documentation

Voir le dossier `docs/` pour la documentation complète et `rapport/rapport_final.md` pour le rapport de projet.

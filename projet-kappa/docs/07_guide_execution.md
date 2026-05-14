# 🚀 Guide d'Exécution — Démonstration Complète

## Prérequis

- ✅ Docker Desktop installé et lancé
- ✅ Python 3.10+ installé
- ✅ `kafka-python` installé (`pip install kafka-python`)

## Étape 1 : Démarrer l'Infrastructure

```bash
# Naviguer dans le dossier du projet
cd chemin/vers/projet-kappa

# Lancer tous les conteneurs en arrière-plan
docker-compose up -d

# Vérifier que tout tourne
docker-compose ps
```

**Résultat attendu** : 4 conteneurs running (zookeeper, kafka, jobmanager, taskmanager)

**Interface Flink** : Ouvrir http://localhost:8081 dans le navigateur

## Étape 2 : Créer les Topics Kafka

```bash
# Créer le topic principal
docker exec -it projet-kappa-kafka-1 kafka-topics --create \
  --topic clics_ecommerce \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000

# Créer le topic filtré
docker exec -it projet-kappa-kafka-1 kafka-topics --create \
  --topic clics_filtres \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=259200000

# Créer le topic stats
docker exec -it projet-kappa-kafka-1 kafka-topics --create \
  --topic stats_produits \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1

# Créer le topic alertes
docker exec -it projet-kappa-kafka-1 kafka-topics --create \
  --topic alertes_marketing \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=604800000

# Vérifier les topics créés
docker exec -it projet-kappa-kafka-1 kafka-topics --list \
  --bootstrap-server localhost:9092
```

## Étape 3 : Lancer le Simulateur (Terminal 1)

```bash
python simulateur.py
```

**Résultat** : Des événements s'affichent toutes les 0.5-2 secondes avec des emojis 👤 (humain) ou 🤖 (bot).

## Étape 4 : Lancer la Topologie de Filtrage (Terminal 2)

```bash
python stream_processing/filtrage.py
```

**Résultat** : Les événements humains sont marqués ✅ TRANSMIS, les bots 🚫 FILTRÉ.

## Étape 5 : Lancer la Topologie d'Agrégation (Terminal 3)

```bash
python stream_processing/agregation.py
```

**Résultat** : Toutes les 30 secondes, un tableau de statistiques s'affiche avec les vues, achats, CA par produit.

## Étape 6 : Lancer la Détection d'Abandon (Terminal 4)

```bash
python stream_processing/detection_abandon.py
```

**Résultat** : Les ajouts panier démarrent un timer. Après 60s sans achat → 🚨 ALERTE ABANDON.

## Étape 7 : Vérifier les Alertes Marketing (Terminal 5)

```bash
docker exec -it projet-kappa-kafka-1 kafka-console-consumer \
  --topic alertes_marketing \
  --from-beginning \
  --bootstrap-server localhost:9092
```

**Résultat** : Les alertes JSON d'abandon de panier s'affichent.

## Étape 8 : Test de Replay (après quelques minutes)

```bash
# Arrêter d'abord le simulateur (Ctrl+C dans Terminal 1)
# Puis lancer le test de replay
python config/test_replay.py
```

**Résultat** : Le script relit tous les événements depuis le début, recalcule les stats, et valide la cohérence.

## Étape 9 : Nettoyage

```bash
# Arrêter tous les conteneurs
docker-compose down

# Supprimer les données (optionnel)
docker-compose down -v
```

## Résumé des Terminaux

| Terminal | Commande | Rôle |
|----------|----------|------|
| 1 | `python simulateur.py` | Génère les événements |
| 2 | `python stream_processing/filtrage.py` | Filtre les bots |
| 3 | `python stream_processing/agregation.py` | Calcule les stats |
| 4 | `python stream_processing/detection_abandon.py` | Détecte les abandons |
| 5 | `kafka-console-consumer --topic alertes_marketing` | Vérifie les alertes |

## Ordre de Démonstration pour la Présentation

1. Montrer Docker Desktop avec les conteneurs qui tournent
2. Lancer le simulateur → montrer les événements
3. Lancer le filtrage → montrer la séparation humains/bots
4. Lancer l'agrégation → montrer les statistiques en temps réel
5. Lancer la détection → attendre une alerte d'abandon 🚨
6. Lancer le replay → montrer que les résultats sont cohérents
7. Conclure : « Voici l'architecture Kappa en action ! »

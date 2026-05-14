#!/bin/bash
# =============================================================================
#  SCRIPT DE CONFIGURATION DES TOPICS KAFKA
# =============================================================================
#  Ce script crée tous les topics nécessaires à l'architecture Kappa
#  avec les configurations de rétention appropriées.
#
#  La RÉTENTION est un concept clé de l'architecture Kappa :
#    → Les événements sont conservés dans Kafka pendant une durée définie
#    → Cela permet le REPLAY : relire tous les événements depuis le début
#    → C'est ce qui remplace le "batch layer" de l'architecture Lambda
#
#  Topics créés :
#    1. clics_ecommerce   : Événements bruts (entrée du système)
#    2. clics_filtres      : Événements après filtrage des bots
#    3. stats_produits     : Statistiques agrégées par produit
#    4. alertes_marketing  : Alertes d'abandon de panier
#
#  Usage :
#    docker exec -it projet-kappa-kafka-1 bash /scripts/setup_topics.sh
#    OU
#    Copier-coller les commandes une par une dans le terminal
#
#  Auteur : Équipe Projet Kappa — ENSA Al Hoceima
# =============================================================================

BROKER="localhost:9092"

echo "=============================================="
echo "  🔧 Configuration des Topics Kafka"
echo "=============================================="

# ──────────────────────────────────────────────────────────────────────────────
# Topic 1 : clics_ecommerce (événements bruts)
# Rétention : 7 jours (604800000 ms) — pour permettre le replay
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "📌 Création du topic 'clics_ecommerce'..."
kafka-topics --create \
  --topic clics_ecommerce \
  --bootstrap-server $BROKER \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config retention.bytes=-1 \
  --if-not-exists
echo "   ✅ Rétention : 7 jours"

# ──────────────────────────────────────────────────────────────────────────────
# Topic 2 : clics_filtres (après filtrage des bots)
# Rétention : 3 jours — données nettoyées
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "📌 Création du topic 'clics_filtres'..."
kafka-topics --create \
  --topic clics_filtres \
  --bootstrap-server $BROKER \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=259200000 \
  --config retention.bytes=-1 \
  --if-not-exists
echo "   ✅ Rétention : 3 jours"

# ──────────────────────────────────────────────────────────────────────────────
# Topic 3 : stats_produits (statistiques agrégées)
# Rétention : 1 jour — résultats de calcul
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "📌 Création du topic 'stats_produits'..."
kafka-topics --create \
  --topic stats_produits \
  --bootstrap-server $BROKER \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config retention.bytes=-1 \
  --if-not-exists
echo "   ✅ Rétention : 1 jour"

# ──────────────────────────────────────────────────────────────────────────────
# Topic 4 : alertes_marketing (alertes d'abandon)
# Rétention : 7 jours — important pour le suivi marketing
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "📌 Création du topic 'alertes_marketing'..."
kafka-topics --create \
  --topic alertes_marketing \
  --bootstrap-server $BROKER \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config retention.bytes=-1 \
  --if-not-exists
echo "   ✅ Rétention : 7 jours"

# ──────────────────────────────────────────────────────────────────────────────
# Vérification : lister tous les topics
# ──────────────────────────────────────────────────────────────────────────────
echo ""
echo "=============================================="
echo "  📋 Liste des topics créés :"
echo "=============================================="
kafka-topics --list --bootstrap-server $BROKER

echo ""
echo "=============================================="
echo "  📊 Détails de la rétention :"
echo "=============================================="
for topic in clics_ecommerce clics_filtres stats_produits alertes_marketing; do
  echo ""
  echo "  → $topic :"
  kafka-topics --describe --topic $topic --bootstrap-server $BROKER 2>/dev/null | head -2
done

echo ""
echo "✅ Configuration terminée !"
echo ""

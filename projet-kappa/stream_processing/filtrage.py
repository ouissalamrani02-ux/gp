"""
=============================================================================
 TOPOLOGIE 1 : FILTRAGE DES ÉVÉNEMENTS
=============================================================================
 Cette topologie est le premier maillon de la chaîne de traitement Kappa.
 
 Son rôle :
   - Lire les événements bruts depuis le topic 'clics_ecommerce'
   - Filtrer les événements générés par des robots/crawlers
   - Écrire les événements humains valides dans 'clics_filtres'

 Pourquoi c'est important dans l'architecture Kappa ?
   → Dans une architecture Kappa, TOUTES les données passent par un unique
     flux de traitement. Le filtrage est la première topologie qui nettoie
     le flux avant que les topologies suivantes (agrégation, détection)
     ne traitent les données.

 Comment ça fonctionne ?
   1. Le consommateur Kafka lit CHAQUE événement du topic source
   2. Il vérifie si le champ 'is_bot' est True ou si le user_agent
      contient un identifiant de robot connu
   3. Si l'événement est humain → il est publié dans 'clics_filtres'
   4. Si l'événement est un bot → il est rejeté (compté pour stats)

 Dans un vrai projet, ceci serait un job Apache Flink (PyFlink).
 Ici on utilise kafka-python pour simplifier la démonstration.

 Auteur : Équipe Projet Kappa — ENSA Al Hoceima
 Date   : 2026
=============================================================================
"""

import json
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BROKER = 'localhost:29092'
TOPIC_SOURCE = 'clics_ecommerce'       # Topic d'entrée (événements bruts)
TOPIC_DESTINATION = 'clics_filtres'     # Topic de sortie (événements nettoyés)

# Liste des user-agents de robots connus
BOTS_CONNUS = [
    "googlebot", "bingbot", "yandexbot", "crawler", "spider",
    "bot", "scraper", "fetch", "archiver"
]


# ──────────────────────────────────────────────────────────────────────────────
# FONCTION DE FILTRAGE
# ──────────────────────────────────────────────────────────────────────────────

def est_un_bot(evenement):
    """
    Détermine si un événement provient d'un robot.
    
    Critères de détection :
    1. Le champ 'is_bot' est explicitement True
    2. Le user_agent contient un identifiant de bot connu
    3. Le user_id commence par 'BOT_'
    
    Retourne True si c'est un bot, False si c'est un humain.
    """
    # Critère 1 : champ is_bot explicite
    if evenement.get("is_bot", False):
        return True

    # Critère 2 : user_agent contient un mot-clé de bot
    user_agent = evenement.get("user_agent", "").lower()
    for bot in BOTS_CONNUS:
        if bot in user_agent:
            return True

    # Critère 3 : user_id de type bot
    user_id = evenement.get("user_id", "")
    if user_id.startswith("BOT_"):
        return True

    return False


# ──────────────────────────────────────────────────────────────────────────────
# TOPOLOGIE PRINCIPALE
# ──────────────────────────────────────────────────────────────────────────────

def main():
    """
    Boucle principale de la topologie de filtrage.
    Lit en continu depuis Kafka et filtre les bots.
    """
    print("=" * 60)
    print("  🔍 TOPOLOGIE 1 : FILTRAGE DES BOTS")
    print("=" * 60)
    print(f"  Source      : {TOPIC_SOURCE}")
    print(f"  Destination : {TOPIC_DESTINATION}")
    print("=" * 60)

    # Création du consommateur Kafka
    consumer = KafkaConsumer(
        TOPIC_SOURCE,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',     # Lire les nouveaux événements
        group_id='filtrage-group'        # Groupe de consommateurs
    )

    # Création du producteur Kafka (pour écrire dans le topic filtré)
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("\n✅ Topologie de filtrage démarrée. En attente d'événements...\n")

    compteur = {"total": 0, "humains": 0, "bots_filtres": 0}

    try:
        for message in consumer:
            evenement = message.value
            compteur["total"] += 1

            if est_un_bot(evenement):
                # ❌ Bot détecté → on ne transmet PAS
                compteur["bots_filtres"] += 1
                print(f"  🚫 BOT FILTRÉ  | {evenement.get('user_id', '?'):>8} | "
                      f"{evenement.get('user_agent', '?')[:40]}")
            else:
                # ✅ Humain → on transmet au topic filtré
                compteur["humains"] += 1
                producer.send(TOPIC_DESTINATION, evenement)
                print(f"  ✅ TRANSMIS     | {evenement.get('user_id', '?'):>8} | "
                      f"{evenement.get('action', '?'):<16} | "
                      f"{evenement.get('produit', '?')}")

            # Affichage des stats périodiques
            if compteur["total"] % 10 == 0:
                taux = (compteur["bots_filtres"] / compteur["total"]) * 100
                print(f"\n  📊 Stats: {compteur['total']} traités | "
                      f"{compteur['humains']} humains | "
                      f"{compteur['bots_filtres']} bots ({taux:.1f}%)\n")

    except KeyboardInterrupt:
        print(f"\n{'=' * 60}")
        print(f"  🛑 Topologie de filtrage arrêtée.")
        print(f"  📊 Bilan final :")
        print(f"     Total traité     : {compteur['total']}")
        print(f"     ✅ Humains transmis : {compteur['humains']}")
        print(f"     🚫 Bots filtrés    : {compteur['bots_filtres']}")
        if compteur["total"] > 0:
            taux = (compteur["bots_filtres"] / compteur["total"]) * 100
            print(f"     📈 Taux de filtrage : {taux:.1f}%")
        print(f"{'=' * 60}")
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()

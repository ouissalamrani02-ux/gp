"""
=============================================================================
 SIMULATEUR E-COMMERCE — Architecture Kappa
=============================================================================
 Ce script simule le comportement des utilisateurs sur une plateforme
 e-commerce fictive. Il génère des événements comportementaux réalistes
 et les envoie dans le topic Kafka 'clics_ecommerce'.

 AMÉLIORATIONS par rapport à la version initiale :
 - Ajout d'événements de robots/bots (pour démontrer le filtrage)
 - Ajout de sessions utilisateur (pour la détection d'abandon)
 - Ajout de prix par produit (pour l'agrégation financière)
 - Scénarios d'achat réalistes (vue → panier → achat OU abandon)

 Auteur : Équipe Projet Kappa — ENSA Al Hoceima
 Date   : 2026
=============================================================================
"""

import json
import time
import random
import uuid
from kafka import KafkaProducer
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'clics_ecommerce'

# Catalogue de produits avec prix (en MAD - Dirham Marocain)
CATALOGUE = {
    "PC_Gamer":           {"prix": 12999.00, "categorie": "Informatique"},
    "Souris_Sans_Fil":    {"prix": 299.00,   "categorie": "Accessoires"},
    "Clavier_Mecanique":  {"prix": 899.00,   "categorie": "Accessoires"},
    "Ecran_4K":           {"prix": 4500.00,  "categorie": "Informatique"},
    "Casque_Audio":       {"prix": 1200.00,  "categorie": "Audio"},
    "Webcam_HD":          {"prix": 650.00,   "categorie": "Accessoires"},
    "SSD_1To":            {"prix": 800.00,   "categorie": "Stockage"},
    "Chaise_Gaming":      {"prix": 3500.00,  "categorie": "Mobilier"},
}

# Types d'actions utilisateur
ACTIONS_HUMAINES = ["vue_produit", "ajout_panier", "achat_valide", "abandon_panier", "recherche"]
ACTIONS_BOT = ["vue_produit", "vue_produit", "vue_produit"]  # Les bots ne font que des vues

# User-agents pour distinguer humains et bots
USER_AGENTS_HUMAINS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) Safari/605.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) Firefox/120.0",
]

USER_AGENTS_BOTS = [
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
    "Bingbot/2.0 (+http://www.bing.com/bingbot.htm)",
    "YandexBot/3.0 (+http://yandex.com/bots)",
    "crawler-bot/1.0",
]

# ──────────────────────────────────────────────────────────────────────────────
# SESSIONS ACTIVES (pour simuler des parcours utilisateur réalistes)
# ──────────────────────────────────────────────────────────────────────────────

sessions_actives = {}

def generer_session_id():
    """Génère un identifiant de session unique."""
    return str(uuid.uuid4())[:8]

def generer_evenement_humain():
    """
    Génère un événement humain réaliste avec un parcours cohérent :
    1. Un utilisateur commence par voir un produit
    2. Il peut ajouter au panier
    3. Il peut valider l'achat OU abandonner le panier
    """
    user_id = f"U{random.randint(1000, 9999)}"
    produit = random.choice(list(CATALOGUE.keys()))
    info_produit = CATALOGUE[produit]

    # Vérifier si l'utilisateur a une session active
    if user_id in sessions_actives:
        session = sessions_actives[user_id]
        produit = session["produit"]
        info_produit = CATALOGUE[produit]

        # Progression du parcours client
        if session["etape"] == "vue_produit":
            action = random.choice(["ajout_panier", "vue_produit", "recherche"])
            if action == "ajout_panier":
                session["etape"] = "ajout_panier"
        elif session["etape"] == "ajout_panier":
            action = random.choices(
                ["achat_valide", "abandon_panier"],
                weights=[40, 60],  # 40% achètent, 60% abandonnent (réaliste)
                k=1
            )[0]
            del sessions_actives[user_id]  # Session terminée
        else:
            action = "vue_produit"
    else:
        # Nouveau visiteur → commence par une vue produit
        action = "vue_produit"
        session_id = generer_session_id()
        sessions_actives[user_id] = {
            "session_id": session_id,
            "produit": produit,
            "etape": "vue_produit"
        }

    evenement = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": sessions_actives.get(user_id, {}).get("session_id", generer_session_id()),
        "action": action,
        "produit": produit,
        "prix": info_produit["prix"],
        "categorie": info_produit["categorie"],
        "user_agent": random.choice(USER_AGENTS_HUMAINS),
        "is_bot": False,
        "timestamp": datetime.now().isoformat()
    }
    return evenement


def generer_evenement_bot():
    """
    Génère un événement de robot/crawler.
    Les bots ne font que des vues produit très rapides.
    Ils seront filtrés par la topologie de filtrage.
    """
    produit = random.choice(list(CATALOGUE.keys()))
    info_produit = CATALOGUE[produit]

    evenement = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"BOT_{random.randint(1, 50)}",
        "session_id": "bot-session",
        "action": "vue_produit",
        "produit": produit,
        "prix": info_produit["prix"],
        "categorie": info_produit["categorie"],
        "user_agent": random.choice(USER_AGENTS_BOTS),
        "is_bot": True,
        "timestamp": datetime.now().isoformat()
    }
    return evenement


# ──────────────────────────────────────────────────────────────────────────────
# PRODUCTEUR KAFKA
# ──────────────────────────────────────────────────────────────────────────────

def main():
    """Point d'entrée principal du simulateur."""

    print("=" * 60)
    print("  🏪 SIMULATEUR E-COMMERCE — Architecture Kappa")
    print("=" * 60)
    print(f"  Broker Kafka : {KAFKA_BROKER}")
    print(f"  Topic        : {TOPIC}")
    print(f"  Produits     : {len(CATALOGUE)}")
    print("=" * 60)

    # Connexion au broker Kafka
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("\n✅ Connexion à Kafka réussie !")
    except Exception as e:
        print(f"\n❌ Erreur de connexion à Kafka : {e}")
        print("   → Vérifiez que Docker est lancé (docker-compose up -d)")
        return

    print("🚀 Démarrage de la simulation... (Ctrl+C pour arrêter)\n")

    compteur = {"total": 0, "humains": 0, "bots": 0}

    try:
        while True:
            # 20% de chance que l'événement soit un bot
            if random.random() < 0.20:
                evenement = generer_evenement_bot()
                compteur["bots"] += 1
                emoji = "🤖"
            else:
                evenement = generer_evenement_humain()
                compteur["humains"] += 1
                emoji = "👤"

            compteur["total"] += 1

            # Envoi dans Kafka
            producer.send(TOPIC, evenement)

            # Affichage formaté
            print(f"  {emoji} [{compteur['total']:04d}] "
                  f"{evenement['user_id']:>8} | "
                  f"{evenement['action']:<16} | "
                  f"{evenement['produit']:<20} | "
                  f"{evenement['prix']:>10.2f} MAD")

            # Pause variable pour simuler un trafic réaliste
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print(f"\n{'=' * 60}")
        print(f"  🛑 Simulateur arrêté.")
        print(f"  📊 Bilan : {compteur['total']} événements envoyés")
        print(f"     👤 Humains : {compteur['humains']}")
        print(f"     🤖 Bots    : {compteur['bots']}")
        print(f"{'=' * 60}")
        producer.close()


if __name__ == "__main__":
    main()
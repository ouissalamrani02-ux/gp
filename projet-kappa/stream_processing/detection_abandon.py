"""
=============================================================================
 TOPOLOGIE 3 : DÉTECTION D'ABANDON DE PANIER
=============================================================================
 Cette topologie détecte les utilisateurs qui ajoutent un produit au panier
 mais ne finalisent pas leur achat dans un délai défini.

 Son rôle :
   - Lire les événements filtrés depuis 'clics_filtres'
   - Suivre les sessions utilisateur (ajout_panier → achat OU abandon)
   - Si un utilisateur fait 'ajout_panier' sans 'achat_valide' dans les
     N secondes suivantes → déclencher une alerte marketing
   - Publier les alertes dans le topic 'alertes_marketing'

 Concept Kappa démontré :
   → Le traitement événementiel avec état (stateful stream processing).
     La topologie maintient un état en mémoire (les paniers en attente)
     et prend des décisions en temps réel basées sur le temps et les
     patterns d'événements.

 Cas d'usage métier :
   → L'équipe Marketing (MOA) veut être alertée en temps réel quand un
     client abandonne son panier. Le système Kappa permet d'envoyer
     immédiatement un email de relance ou une notification push.

 Auteur : Équipe Projet Kappa — ENSA Al Hoceima
 Date   : 2026
=============================================================================
"""

import json
import time
import threading
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BROKER = 'localhost:29092'
TOPIC_SOURCE = 'clics_filtres'          # Événements filtrés
TOPIC_ALERTES = 'alertes_marketing'     # Alertes d'abandon de panier
TIMEOUT_ABANDON_SECONDES = 60           # Délai avant de considérer un abandon


# ──────────────────────────────────────────────────────────────────────────────
# ÉTAT DES PANIERS EN ATTENTE
# ──────────────────────────────────────────────────────────────────────────────

# Structure : { user_id: { produit, prix, timestamp_ajout, timer } }
paniers_en_attente = {}
lock = threading.Lock()


# ──────────────────────────────────────────────────────────────────────────────
# FONCTIONS DE DÉTECTION
# ──────────────────────────────────────────────────────────────────────────────

def on_timeout_abandon(user_id, produit, prix, producer):
    """
    Callback appelé quand le délai d'abandon est atteint.
    
    Si l'utilisateur n'a toujours pas acheté après le timeout,
    on génère une alerte marketing.
    """
    with lock:
        # Vérifier que le panier est toujours en attente
        if user_id in paniers_en_attente:
            panier = paniers_en_attente[user_id]
            if panier["produit"] == produit:
                # ⚠️ ABANDON DÉTECTÉ → Générer l'alerte
                alerte = {
                    "type": "alerte_abandon_panier",
                    "user_id": user_id,
                    "produit": produit,
                    "prix": prix,
                    "duree_attente_secondes": TIMEOUT_ABANDON_SECONDES,
                    "action_recommandee": "Envoyer email de relance avec -10%",
                    "priorite": "haute" if prix > 2000 else "normale",
                    "timestamp_ajout": panier["timestamp_ajout"],
                    "timestamp_alerte": datetime.now().isoformat()
                }

                producer.send(TOPIC_ALERTES, alerte)

                print(f"\n  🚨 ALERTE ABANDON !")
                print(f"     👤 Utilisateur : {user_id}")
                print(f"     🛒 Produit     : {produit}")
                print(f"     💰 Valeur      : {prix:.2f} MAD")
                print(f"     ⏱️  Attente     : {TIMEOUT_ABANDON_SECONDES}s sans achat")
                print(f"     📧 Action      : {alerte['action_recommandee']}")
                print()

                # Nettoyer l'état
                del paniers_en_attente[user_id]


def traiter_evenement(evenement, producer):
    """
    Traite un événement et met à jour l'état des paniers.
    
    Logique :
      - ajout_panier  → Démarre un timer d'abandon
      - achat_valide  → Annule le timer (l'utilisateur a acheté ✅)
      - abandon_panier → Déclenche immédiatement l'alerte
    """
    user_id = evenement.get("user_id", "")
    action = evenement.get("action", "")
    produit = evenement.get("produit", "")
    prix = evenement.get("prix", 0.0)

    with lock:
        if action == "ajout_panier":
            # Annuler un ancien timer s'il existe
            if user_id in paniers_en_attente:
                ancien = paniers_en_attente[user_id]
                if "timer" in ancien:
                    ancien["timer"].cancel()

            # Démarrer un nouveau timer d'abandon
            timer = threading.Timer(
                TIMEOUT_ABANDON_SECONDES,
                on_timeout_abandon,
                args=[user_id, produit, prix, producer]
            )
            timer.daemon = True
            timer.start()

            paniers_en_attente[user_id] = {
                "produit": produit,
                "prix": prix,
                "timestamp_ajout": datetime.now().isoformat(),
                "timer": timer
            }

            print(f"  🛒 PANIER     | {user_id:>8} | {produit:<20} | "
                  f"{prix:>10.2f} MAD | ⏱️ Timer {TIMEOUT_ABANDON_SECONDES}s")

        elif action == "achat_valide":
            # L'utilisateur a acheté → annuler le timer d'abandon
            if user_id in paniers_en_attente:
                panier = paniers_en_attente[user_id]
                if "timer" in panier:
                    panier["timer"].cancel()
                del paniers_en_attente[user_id]

            print(f"  ✅ ACHAT      | {user_id:>8} | {produit:<20} | "
                  f"{prix:>10.2f} MAD | Timer annulé ✓")

        elif action == "abandon_panier":
            # Abandon explicite → alerte immédiate
            if user_id in paniers_en_attente:
                panier = paniers_en_attente[user_id]
                if "timer" in panier:
                    panier["timer"].cancel()
                del paniers_en_attente[user_id]

            # Générer l'alerte immédiatement
            alerte = {
                "type": "alerte_abandon_panier",
                "user_id": user_id,
                "produit": produit,
                "prix": prix,
                "duree_attente_secondes": 0,
                "action_recommandee": "Envoyer notification push immédiate",
                "priorite": "haute" if prix > 2000 else "normale",
                "timestamp_ajout": evenement.get("timestamp", ""),
                "timestamp_alerte": datetime.now().isoformat()
            }
            producer.send(TOPIC_ALERTES, alerte)

            print(f"  ❌ ABANDON    | {user_id:>8} | {produit:<20} | "
                  f"{prix:>10.2f} MAD | 🚨 Alerte envoyée")

        else:
            # Autres événements (vue_produit, recherche) → pas d'action
            print(f"  👁️  VUE/AUTRE | {user_id:>8} | {produit:<20} | "
                  f"{action}")


# ──────────────────────────────────────────────────────────────────────────────
# TOPOLOGIE PRINCIPALE
# ──────────────────────────────────────────────────────────────────────────────

def main():
    """Boucle principale de la détection d'abandon de panier."""

    print("=" * 60)
    print("  🚨 TOPOLOGIE 3 : DÉTECTION D'ABANDON DE PANIER")
    print("=" * 60)
    print(f"  Source      : {TOPIC_SOURCE}")
    print(f"  Alertes     : {TOPIC_ALERTES}")
    print(f"  Timeout     : {TIMEOUT_ABANDON_SECONDES} secondes")
    print("=" * 60)

    consumer = KafkaConsumer(
        TOPIC_SOURCE,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='detection-abandon-group'
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("\n✅ Détection d'abandon démarrée. En attente d'événements...\n")

    compteur = {"total": 0, "alertes": 0, "achats_sauves": 0}

    try:
        for message in consumer:
            evenement = message.value
            compteur["total"] += 1
            traiter_evenement(evenement, producer)

            # Affichage périodique des stats
            if compteur["total"] % 20 == 0:
                with lock:
                    en_attente = len(paniers_en_attente)
                print(f"\n  📊 Stats: {compteur['total']} traités | "
                      f"🛒 {en_attente} paniers en attente\n")

    except KeyboardInterrupt:
        # Annuler tous les timers
        with lock:
            for uid, panier in paniers_en_attente.items():
                if "timer" in panier:
                    panier["timer"].cancel()

        print(f"\n{'=' * 60}")
        print(f"  🛑 Détection d'abandon arrêtée.")
        print(f"  📊 Total traité : {compteur['total']}")
        print(f"  🛒 Paniers restants en attente : {len(paniers_en_attente)}")
        print(f"{'=' * 60}")
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()

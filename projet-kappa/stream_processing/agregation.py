"""
=============================================================================
 TOPOLOGIE 2 : AGRÉGATION TEMPS RÉEL
=============================================================================
 Cette topologie calcule des statistiques en temps réel sur les événements
 filtrés (uniquement les événements humains).

 Son rôle :
   - Lire les événements propres depuis 'clics_filtres'
   - Calculer par produit : nombre de vues, ajouts panier, achats, abandons
   - Calculer le chiffre d'affaires en temps réel par produit
   - Calculer le taux de conversion (achats / vues) par produit
   - Publier les statistiques agrégées dans 'stats_produits'

 Concept Kappa démontré :
   → L'agrégation continue est le cœur de l'architecture Kappa. Au lieu
     d'avoir un batch layer (comme dans Lambda), on calcule les agrégats
     EN CONTINU sur le flux unique. Si on a besoin de recalculer, on 
     rejoue le flux depuis le début (replay).

 Fenêtrage :
   → On utilise des fenêtres temporelles de 30 secondes pour regrouper
     les événements et calculer des métriques par intervalle.

 Auteur : Équipe Projet Kappa — ENSA Al Hoceima
 Date   : 2026
=============================================================================
"""

import json
import time
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BROKER = 'localhost:29092'
TOPIC_SOURCE = 'clics_filtres'          # Événements filtrés (sans bots)
TOPIC_STATS = 'stats_produits'          # Statistiques agrégées
FENETRE_SECONDES = 30                   # Taille de la fenêtre d'agrégation


# ──────────────────────────────────────────────────────────────────────────────
# STRUCTURES D'AGRÉGATION
# ──────────────────────────────────────────────────────────────────────────────

# Statistiques globales (depuis le démarrage)
stats_globales = defaultdict(lambda: {
    "vues": 0,
    "ajouts_panier": 0,
    "achats": 0,
    "abandons": 0,
    "chiffre_affaires": 0.0,
    "recherches": 0
})

# Statistiques de la fenêtre courante
stats_fenetre = defaultdict(lambda: {
    "vues": 0,
    "ajouts_panier": 0,
    "achats": 0,
    "abandons": 0,
    "chiffre_affaires": 0.0
})

derniere_fenetre = time.time()


# ──────────────────────────────────────────────────────────────────────────────
# FONCTIONS D'AGRÉGATION
# ──────────────────────────────────────────────────────────────────────────────

def agreger_evenement(evenement):
    """
    Met à jour les compteurs d'agrégation pour un événement donné.
    
    Mapping des actions vers les compteurs :
      - vue_produit    → vues + 1
      - ajout_panier   → ajouts_panier + 1
      - achat_valide   → achats + 1, chiffre_affaires + prix
      - abandon_panier → abandons + 1
      - recherche      → recherches + 1
    """
    produit = evenement.get("produit", "Inconnu")
    action = evenement.get("action", "")
    prix = evenement.get("prix", 0.0)

    # Mise à jour des stats globales
    if action == "vue_produit":
        stats_globales[produit]["vues"] += 1
        stats_fenetre[produit]["vues"] += 1
    elif action == "ajout_panier":
        stats_globales[produit]["ajouts_panier"] += 1
        stats_fenetre[produit]["ajouts_panier"] += 1
    elif action == "achat_valide":
        stats_globales[produit]["achats"] += 1
        stats_globales[produit]["chiffre_affaires"] += prix
        stats_fenetre[produit]["achats"] += 1
        stats_fenetre[produit]["chiffre_affaires"] += prix
    elif action == "abandon_panier":
        stats_globales[produit]["abandons"] += 1
        stats_fenetre[produit]["abandons"] += 1
    elif action == "recherche":
        stats_globales[produit]["recherches"] += 1


def calculer_taux_conversion(stats):
    """Calcule le taux de conversion : achats / vues × 100."""
    if stats["vues"] == 0:
        return 0.0
    return (stats["achats"] / stats["vues"]) * 100


def publier_stats_fenetre(producer):
    """
    Publie les statistiques de la fenêtre courante dans Kafka.
    C'est le résultat de l'agrégation qui peut être consommé
    par un dashboard ou un autre service.
    """
    global stats_fenetre, derniere_fenetre

    maintenant = datetime.now().isoformat()

    for produit, stats in stats_fenetre.items():
        if any(v > 0 for v in stats.values()):
            message_stats = {
                "type": "stats_fenetre",
                "produit": produit,
                "fenetre_debut": datetime.fromtimestamp(derniere_fenetre).isoformat(),
                "fenetre_fin": maintenant,
                "duree_secondes": FENETRE_SECONDES,
                "stats": dict(stats),
                "taux_conversion": calculer_taux_conversion(stats),
                "timestamp": maintenant
            }
            producer.send(TOPIC_STATS, message_stats)

    # Réinitialiser la fenêtre
    stats_fenetre.clear()
    derniere_fenetre = time.time()


def afficher_tableau_stats():
    """Affiche un tableau formaté des statistiques globales."""
    print(f"\n  {'─' * 90}")
    print(f"  {'Produit':<22} {'Vues':>6} {'Panier':>8} {'Achats':>8} "
          f"{'Abandons':>10} {'CA (MAD)':>12} {'Conv.%':>8}")
    print(f"  {'─' * 90}")

    ca_total = 0
    for produit in sorted(stats_globales.keys()):
        s = stats_globales[produit]
        taux = calculer_taux_conversion(s)
        ca_total += s["chiffre_affaires"]
        print(f"  {produit:<22} {s['vues']:>6} {s['ajouts_panier']:>8} "
              f"{s['achats']:>8} {s['abandons']:>10} "
              f"{s['chiffre_affaires']:>12,.2f} {taux:>7.1f}%")

    print(f"  {'─' * 90}")
    print(f"  {'TOTAL':>22} {'':>6} {'':>8} {'':>8} {'':>10} {ca_total:>12,.2f}")
    print(f"  {'─' * 90}\n")


# ──────────────────────────────────────────────────────────────────────────────
# TOPOLOGIE PRINCIPALE
# ──────────────────────────────────────────────────────────────────────────────

def main():
    """Boucle principale de la topologie d'agrégation."""

    global derniere_fenetre

    print("=" * 60)
    print("  📊 TOPOLOGIE 2 : AGRÉGATION TEMPS RÉEL")
    print("=" * 60)
    print(f"  Source      : {TOPIC_SOURCE}")
    print(f"  Destination : {TOPIC_STATS}")
    print(f"  Fenêtre     : {FENETRE_SECONDES} secondes")
    print("=" * 60)

    consumer = KafkaConsumer(
        TOPIC_SOURCE,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='agregation-group',
        consumer_timeout_ms=1000  # Timeout pour vérifier la fenêtre
    )

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print("\n✅ Topologie d'agrégation démarrée. En attente d'événements...\n")

    compteur_total = 0
    derniere_fenetre = time.time()

    try:
        while True:
            # Consommer les messages disponibles
            messages = consumer.poll(timeout_ms=1000)

            for tp, records in messages.items():
                for message in records:
                    evenement = message.value
                    compteur_total += 1
                    agreger_evenement(evenement)

                    print(f"  📥 [{compteur_total:04d}] "
                          f"{evenement.get('user_id', '?'):>8} | "
                          f"{evenement.get('action', '?'):<16} | "
                          f"{evenement.get('produit', '?'):<20} | "
                          f"{evenement.get('prix', 0):>10.2f} MAD")

            # Vérifier si la fenêtre est terminée
            if time.time() - derniere_fenetre >= FENETRE_SECONDES:
                print(f"\n  ⏰ FIN DE FENÊTRE ({FENETRE_SECONDES}s) — "
                      f"Publication des statistiques...")
                publier_stats_fenetre(producer)
                afficher_tableau_stats()

    except KeyboardInterrupt:
        print(f"\n{'=' * 60}")
        print(f"  🛑 Topologie d'agrégation arrêtée.")
        print(f"  📊 Bilan final : {compteur_total} événements agrégés")
        print(f"{'=' * 60}")
        afficher_tableau_stats()
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()

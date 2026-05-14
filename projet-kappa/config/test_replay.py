"""
=============================================================================
 TEST DE REPLAY ET VALIDATION DE COHÉRENCE
=============================================================================
 Ce script démontre la fonctionnalité de REPLAY de l'architecture Kappa.

 Le REPLAY est LE concept fondamental qui différencie Kappa de Lambda :
   → Au lieu d'avoir un batch layer qui recalcule périodiquement,
     Kappa rejoue simplement les événements depuis le début du log Kafka.
   → Le résultat du replay doit être IDENTIQUE au résultat temps réel.
   → C'est cette cohérence qui valide l'architecture.

 Ce que fait ce script :
   1. Lit TOUS les événements depuis le début (--from-beginning)
   2. Recalcule les statistiques d'agrégation (comme la topologie 2)
   3. Affiche les résultats recalculés
   4. Compare avec les résultats temps réel (si disponibles)
   5. Valide que les résultats sont cohérents

 C'est la preuve que l'architecture Kappa fonctionne correctement :
   Si replay == temps réel → le système est cohérent ✅
   Si replay ≠ temps réel → il y a un problème de cohérence ❌

 Auteur : Équipe Projet Kappa — ENSA Al Hoceima
 Date   : 2026
=============================================================================
"""

import json
from collections import defaultdict
from kafka import KafkaConsumer
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────

KAFKA_BROKER = 'localhost:29092'
TOPIC_BRUT = 'clics_ecommerce'
TOPIC_FILTRE = 'clics_filtres'
TOPIC_STATS = 'stats_produits'

# Mots-clés de bots (même logique que filtrage.py)
BOTS_CONNUS = ["googlebot", "bingbot", "yandexbot", "crawler", "spider",
               "bot", "scraper", "fetch", "archiver"]


# ──────────────────────────────────────────────────────────────────────────────
# FONCTIONS UTILITAIRES
# ──────────────────────────────────────────────────────────────────────────────

def est_un_bot(evenement):
    """Même logique de détection de bot que filtrage.py."""
    if evenement.get("is_bot", False):
        return True
    user_agent = evenement.get("user_agent", "").lower()
    for b in BOTS_CONNUS:
        if b in user_agent:
            return True
    if evenement.get("user_id", "").startswith("BOT_"):
        return True
    return False


def lire_tous_les_evenements(topic, timeout_ms=5000):
    """
    Lit TOUS les événements d'un topic Kafka depuis le début.
    C'est l'opération de REPLAY.
    
    Le paramètre auto_offset_reset='earliest' est la clé :
    il force Kafka à renvoyer les événements depuis le tout début.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id=None,              # Pas de groupe → lecture indépendante
        consumer_timeout_ms=timeout_ms
    )

    evenements = []
    for message in consumer:
        evenements.append(message.value)

    consumer.close()
    return evenements


# ──────────────────────────────────────────────────────────────────────────────
# REPLAY : Recalculer les statistiques depuis le début
# ──────────────────────────────────────────────────────────────────────────────

def replay_agregation(evenements):
    """
    Rejoue tous les événements et recalcule les agrégations.
    C'est exactement le même algorithme que la topologie d'agrégation,
    mais appliqué à l'historique complet.
    """
    stats = defaultdict(lambda: {
        "vues": 0, "ajouts_panier": 0, "achats": 0,
        "abandons": 0, "chiffre_affaires": 0.0
    })

    for evt in evenements:
        produit = evt.get("produit", "Inconnu")
        action = evt.get("action", "")
        prix = evt.get("prix", 0.0)

        if action == "vue_produit":
            stats[produit]["vues"] += 1
        elif action == "ajout_panier":
            stats[produit]["ajouts_panier"] += 1
        elif action == "achat_valide":
            stats[produit]["achats"] += 1
            stats[produit]["chiffre_affaires"] += prix
        elif action == "abandon_panier":
            stats[produit]["abandons"] += 1

    return dict(stats)


def replay_filtrage(evenements):
    """
    Rejoue le filtrage sur tous les événements bruts.
    Sépare les humains des bots.
    """
    humains = []
    bots = []

    for evt in evenements:
        if est_un_bot(evt):
            bots.append(evt)
        else:
            humains.append(evt)

    return humains, bots


# ──────────────────────────────────────────────────────────────────────────────
# VALIDATION DE COHÉRENCE
# ──────────────────────────────────────────────────────────────────────────────

def valider_coherence(stats_replay, stats_temps_reel):
    """
    Compare les statistiques du replay avec celles du temps réel.
    Si elles sont identiques → le système Kappa est cohérent.
    """
    print("\n  🔍 VALIDATION DE COHÉRENCE")
    print(f"  {'─' * 70}")

    coherent = True
    for produit in set(list(stats_replay.keys()) + list(stats_temps_reel.keys())):
        sr = stats_replay.get(produit, {})
        st = stats_temps_reel.get(produit, {})

        for metrique in ["vues", "ajouts_panier", "achats", "abandons"]:
            val_replay = sr.get(metrique, 0)
            val_rt = st.get(metrique, 0)

            if val_replay == val_rt:
                status = "✅"
            else:
                status = "❌"
                coherent = False

            print(f"  {status} {produit:<20} | {metrique:<16} | "
                  f"Replay: {val_replay:>5} | Temps réel: {val_rt:>5}")

    print(f"  {'─' * 70}")
    if coherent:
        print("  ✅ RÉSULTAT : Le système est COHÉRENT — Replay = Temps Réel")
    else:
        print("  ❌ RÉSULTAT : INCOHÉRENCE détectée — Vérifier les topologies")

    return coherent


# ──────────────────────────────────────────────────────────────────────────────
# PROGRAMME PRINCIPAL
# ──────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  🔄 TEST DE REPLAY — Validation de Cohérence Kappa")
    print("=" * 70)

    # ── Étape 1 : Replay des événements bruts ────────────────────────────
    print("\n  📥 Étape 1 : Lecture de TOUS les événements depuis le début...")
    print(f"     (Topic: {TOPIC_BRUT})")

    evenements_bruts = lire_tous_les_evenements(TOPIC_BRUT, timeout_ms=10000)
    print(f"     → {len(evenements_bruts)} événements lus depuis le log Kafka")

    if len(evenements_bruts) == 0:
        print("\n  ⚠️  Aucun événement trouvé. Lancez d'abord le simulateur !")
        return

    # ── Étape 2 : Replay du filtrage ─────────────────────────────────────
    print("\n  🔍 Étape 2 : Replay du filtrage (séparation humains/bots)...")
    humains, bots = replay_filtrage(evenements_bruts)
    print(f"     → {len(humains)} événements humains")
    print(f"     → {len(bots)} événements de bots filtrés")
    taux_bots = (len(bots) / len(evenements_bruts)) * 100 if evenements_bruts else 0
    print(f"     → Taux de bots : {taux_bots:.1f}%")

    # ── Étape 3 : Replay de l'agrégation ─────────────────────────────────
    print("\n  📊 Étape 3 : Replay de l'agrégation (recalcul complet)...")
    stats_replay = replay_agregation(humains)

    print(f"\n  {'─' * 80}")
    print(f"  {'Produit':<22} {'Vues':>6} {'Panier':>8} {'Achats':>8} "
          f"{'Abandons':>10} {'CA (MAD)':>12} {'Conv.%':>8}")
    print(f"  {'─' * 80}")

    ca_total = 0
    total_vues = 0
    total_achats = 0

    for produit in sorted(stats_replay.keys()):
        s = stats_replay[produit]
        taux = (s["achats"] / s["vues"] * 100) if s["vues"] > 0 else 0
        ca_total += s["chiffre_affaires"]
        total_vues += s["vues"]
        total_achats += s["achats"]

        print(f"  {produit:<22} {s['vues']:>6} {s['ajouts_panier']:>8} "
              f"{s['achats']:>8} {s['abandons']:>10} "
              f"{s['chiffre_affaires']:>12,.2f} {taux:>7.1f}%")

    print(f"  {'─' * 80}")
    taux_global = (total_achats / total_vues * 100) if total_vues > 0 else 0
    print(f"  {'TOTAL':<22} {total_vues:>6} {'':>8} {total_achats:>8} "
          f"{'':>10} {ca_total:>12,.2f} {taux_global:>7.1f}%")
    print(f"  {'─' * 80}")

    # ── Étape 4 : Comparaison avec temps réel ────────────────────────────
    print("\n  🔄 Étape 4 : Comparaison avec les événements filtrés en temps réel...")

    evenements_filtres = lire_tous_les_evenements(TOPIC_FILTRE, timeout_ms=5000)
    print(f"     → {len(evenements_filtres)} événements dans le topic filtré")

    if len(evenements_filtres) > 0:
        stats_temps_reel = replay_agregation(evenements_filtres)
        valider_coherence(stats_replay, stats_temps_reel)
    else:
        print("     ⚠️  Pas de données en temps réel pour comparer.")
        print("     → Lancez la topologie de filtrage pour remplir 'clics_filtres'")

    # ── Résumé ───────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  📋 RÉSUMÉ DU REPLAY")
    print(f"{'=' * 70}")
    print(f"  Événements bruts       : {len(evenements_bruts)}")
    print(f"  Humains (après filtre) : {len(humains)}")
    print(f"  Bots filtrés           : {len(bots)}")
    print(f"  Chiffre d'affaires     : {ca_total:,.2f} MAD")
    print(f"  Taux de conversion     : {taux_global:.1f}%")
    print(f"{'=' * 70}")
    print(f"\n  💡 Ce résultat prouve que l'architecture Kappa permet de")
    print(f"     recalculer N'IMPORTE QUEL résultat en rejouant le log.")
    print(f"     C'est l'alternative au batch layer de Lambda.\n")


if __name__ == "__main__":
    main()

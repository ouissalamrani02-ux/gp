# 🔧 Implémentation Technique — Guide Détaillé

## 1. Vue d'ensemble des Composants

Notre architecture Kappa est composée de **5 composants principaux** :

| # | Composant | Fichier | Rôle |
|---|-----------|---------|------|
| 1 | Simulateur E-commerce | `simulateur.py` | Génère les événements clients |
| 2 | Topologie de Filtrage | `stream_processing/filtrage.py` | Élimine les bots |
| 3 | Topologie d'Agrégation | `stream_processing/agregation.py` | Calcule les stats en temps réel |
| 4 | Détection d'Abandon | `stream_processing/detection_abandon.py` | Alerte quand un client abandonne |
| 5 | Test de Replay | `config/test_replay.py` | Valide la cohérence du système |

---

## 2. Composant 1 : Le Simulateur (`simulateur.py`)

### Objectif
Simuler le comportement des utilisateurs sur notre plateforme e-commerce fictive. C'est le **producteur de données** qui alimente le flux Kappa.

### Fonctionnement

```
 Utilisateurs simulés (humains + bots)
         │
         ▼
 ┌───────────────────┐
 │   SIMULATEUR.PY    │
 │                   │
 │ 1. Génère un user │
 │ 2. Choisit action │
 │ 3. Crée l'event   │
 │ 4. Envoie à Kafka │
 └────────┬──────────┘
          │ KafkaProducer
          ▼
 ┌───────────────────┐
 │  Topic Kafka :    │
 │  clics_ecommerce  │
 └───────────────────┘
```

### Structure d'un événement

```json
{
    "event_id": "a1b2c3d4-e5f6-...",
    "user_id": "U1234",
    "session_id": "ab12cd34",
    "action": "ajout_panier",
    "produit": "PC_Gamer",
    "prix": 12999.00,
    "categorie": "Informatique",
    "user_agent": "Mozilla/5.0 (Windows NT 10.0)...",
    "is_bot": false,
    "timestamp": "2026-05-13T21:43:55.357066"
}
```

### Types d'événements

| Action | Description | Fréquence |
|--------|-------------|-----------|
| `vue_produit` | L'utilisateur consulte un produit | ~40% |
| `ajout_panier` | L'utilisateur ajoute au panier | ~20% |
| `achat_valide` | L'utilisateur finalise l'achat | ~8% |
| `abandon_panier` | L'utilisateur abandonne son panier | ~12% |
| `recherche` | L'utilisateur fait une recherche | ~10% |
| `vue_produit` (bot) | Un robot scanne les produits | ~20% |

### Catalogue de produits

| Produit | Prix (MAD) | Catégorie |
|---------|-----------|-----------|
| PC_Gamer | 12 999,00 | Informatique |
| Souris_Sans_Fil | 299,00 | Accessoires |
| Clavier_Mecanique | 899,00 | Accessoires |
| Ecran_4K | 4 500,00 | Informatique |
| Casque_Audio | 1 200,00 | Audio |
| Webcam_HD | 650,00 | Accessoires |
| SSD_1To | 800,00 | Stockage |
| Chaise_Gaming | 3 500,00 | Mobilier |

### Pourquoi des bots ?
- 20% des événements sont des **robots/crawlers** (Googlebot, Bingbot, etc.)
- C'est réaliste : sur un vrai site e-commerce, ~30-40% du trafic vient de bots
- Cela permet de **démontrer la topologie de filtrage**

---

## 3. Composant 2 : Topologie de Filtrage (`filtrage.py`)

### Objectif
Premier maillon de la chaîne de traitement. Sépare les événements humains des événements de bots.

### Fonctionnement

```
 clics_ecommerce ──► FILTRAGE ──► clics_filtres
                         │
                         └──► (bots rejetés → compteur)
```

### Critères de détection des bots

1. **Champ `is_bot`** : si `true` → bot
2. **User-agent** : contient "googlebot", "bingbot", "crawler", etc.
3. **User ID** : commence par "BOT_"

### Métriques
- Nombre total d'événements traités
- Nombre d'événements humains transmis
- Nombre de bots filtrés
- Taux de filtrage (%)

---

## 4. Composant 3 : Topologie d'Agrégation (`agregation.py`)

### Objectif
Calculer des **statistiques en temps réel** par produit, en utilisant des **fenêtres temporelles**.

### Concept de Fenêtrage (Windowing)

```
 Temps ──────────────────────────────────────────────►

 ├────── Fenêtre 1 (30s) ──────┤────── Fenêtre 2 (30s) ──────┤
 │  evt1  evt2  evt3  evt4     │  evt5  evt6  evt7            │
 │  → Calcul stats fenêtre 1   │  → Calcul stats fenêtre 2   │
 │  → Publish dans Kafka       │  → Publish dans Kafka       │
```

### Métriques calculées par produit

| Métrique | Calcul | Utilité métier |
|----------|--------|----------------|
| **Vues** | count(vue_produit) | Popularité du produit |
| **Ajouts panier** | count(ajout_panier) | Intention d'achat |
| **Achats** | count(achat_valide) | Ventes réelles |
| **Abandons** | count(abandon_panier) | Perte potentielle |
| **Chiffre d'affaires** | sum(prix × achats) | Revenu en temps réel |
| **Taux de conversion** | achats / vues × 100 | Efficacité commerciale |

### Sortie (topic `stats_produits`)

```json
{
    "type": "stats_fenetre",
    "produit": "PC_Gamer",
    "fenetre_debut": "2026-05-13T21:43:00",
    "fenetre_fin": "2026-05-13T21:43:30",
    "duree_secondes": 30,
    "stats": {
        "vues": 5,
        "ajouts_panier": 2,
        "achats": 1,
        "abandons": 1,
        "chiffre_affaires": 12999.00
    },
    "taux_conversion": 20.0
}
```

---

## 5. Composant 4 : Détection d'Abandon de Panier (`detection_abandon.py`)

### Objectif
Détecter en temps réel les utilisateurs qui ajoutent au panier mais ne finalisent pas l'achat, et générer des **alertes marketing**.

### Concept : Traitement avec État (Stateful Processing)

```
 Événement "ajout_panier" pour U123
         │
         ▼
 ┌───────────────────────────┐
 │  ÉTAT EN MÉMOIRE          │
 │  paniers_en_attente = {   │
 │    "U123": {              │
 │      produit: "PC_Gamer", │
 │      prix: 12999,         │
 │      timer: 60s           │  ← Timer démarre
 │    }                      │
 │  }                        │
 └───────────────────────────┘

 CAS 1: "achat_valide" arrive avant 60s
   → Timer annulé ✅ → Pas d'alerte

 CAS 2: 60 secondes passent sans achat
   → 🚨 ALERTE ABANDON → Publié dans alertes_marketing

 CAS 3: "abandon_panier" explicite
   → 🚨 ALERTE IMMÉDIATE → Publié dans alertes_marketing
```

### Sortie (topic `alertes_marketing`)

```json
{
    "type": "alerte_abandon_panier",
    "user_id": "U1234",
    "produit": "PC_Gamer",
    "prix": 12999.00,
    "duree_attente_secondes": 60,
    "action_recommandee": "Envoyer email de relance avec -10%",
    "priorite": "haute",
    "timestamp_alerte": "2026-05-13T21:44:55"
}
```

### Règles métier
- **Priorité haute** : si le prix du produit > 2000 MAD
- **Priorité normale** : si le prix ≤ 2000 MAD
- **Action recommandée** : 
  - Abandon par timeout → email de relance avec réduction
  - Abandon explicite → notification push immédiate

---

## 6. Composant 5 : Test de Replay (`test_replay.py`)

### Objectif
Prouver que l'architecture Kappa permet de **recalculer les résultats** en rejouant le log d'événements.

### Le Replay — Concept Clé

```
 TEMPS RÉEL (pendant l'exécution):
 ─────────────────────────────────────────────►
 evt1 → evt2 → evt3 → ... → evtN → Résultat A

 REPLAY (après, depuis le début):
 ─────────────────────────────────────────────►
 evt1 → evt2 → evt3 → ... → evtN → Résultat B

 VALIDATION: Résultat A == Résultat B ? ✅ Cohérent !
```

### Étapes du test

1. **Lire tous les événements** depuis le début (`auto_offset_reset='earliest'`)
2. **Rejouer le filtrage** : séparer humains et bots
3. **Rejouer l'agrégation** : recalculer les statistiques
4. **Comparer** les résultats replay vs temps réel
5. **Conclure** : cohérent ou non

---

## 7. Flux de Données Complet

```
 ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌───────────┐
 │SIMULATEUR│───►│  KAFKA   │───►│FILTRAGE  │───►│  KAFKA    │
 │          │    │clics_    │    │(bots out)│    │clics_     │
 │Producteur│    │ecommerce │    │          │    │filtres    │
 └──────────┘    └──────────┘    └──────────┘    └─────┬─────┘
                                                       │
                                           ┌───────────┼───────────┐
                                           │                       │
                                     ┌─────▼─────┐          ┌─────▼─────┐
                                     │AGRÉGATION │          │DÉTECTION  │
                                     │Stats/prod │          │Abandon    │
                                     │           │          │Panier     │
                                     └─────┬─────┘          └─────┬─────┘
                                           │                       │
                                     ┌─────▼─────┐          ┌─────▼─────┐
                                     │  KAFKA    │          │  KAFKA    │
                                     │stats_     │          │alertes_   │
                                     │produits   │          │marketing  │
                                     └───────────┘          └───────────┘
```

## 8. Topics Kafka et Rétention

| Topic | Contenu | Rétention | Partitions |
|-------|---------|-----------|------------|
| `clics_ecommerce` | Événements bruts (humains + bots) | 7 jours | 3 |
| `clics_filtres` | Événements nettoyés (humains seulement) | 3 jours | 3 |
| `stats_produits` | Statistiques agrégées par fenêtre | 1 jour | 1 |
| `alertes_marketing` | Alertes d'abandon de panier | 7 jours | 1 |

# 📸 Guide Complet : Captures d'Écran Requises

Ce document est un guide pas-à-pas destiné à l'équipe projet pour réaliser les captures d'écran manquantes. Ces images sont **cruciales** car elles prouvent que le cœur de notre architecture Kappa (le traitement de flux) fonctionne correctement. Elles seront utilisées dans le rapport LaTeX final et pour la présentation devant le professeur.

## 📁 Où sauvegarder les images ?
Toutes les captures d'écran doivent être sauvegardées dans le dossier :
👉 `projet-kappa/screenshots/`

---

## 📸 Capture 1 : La Topologie de Filtrage (Séparation Humains/Bots)

**Pourquoi c'est important :** Cela prouve que notre système est capable d'analyser un flux d'événements en temps réel et d'appliquer une règle métier (éliminer le trafic non pertinent).

1. **Préparation :**
   - Assurez-vous que Docker tourne (`docker-compose up -d`).
   - Ouvrez un premier terminal et lancez le simulateur : `python simulateur.py`
2. **Action :**
   - Ouvrez un **deuxième terminal** côte à côte.
   - Lancez le script de filtrage : `python stream_processing/filtrage.py`
   - Laissez tourner quelques secondes jusqu'à voir apparaître des messages "✅ TRANSMIS" et "🚫 BOT FILTRÉ".
3. **À capturer :** Prenez une capture d'écran montrant de préférence les deux terminaux (le simulateur qui envoie et le filtrage qui trie).
4. **Nom du fichier :** `filtrage_bots.png`

---

## 📸 Capture 2 : La Topologie d'Agrégation (Statistiques en Temps Réel)

**Pourquoi c'est important :** C'est le cœur métier de l'architecture. Cela démontre le traitement par fenêtres glissantes (windowing).

1. **Préparation :**
   - Le simulateur et le script de filtrage (étape précédente) doivent continuer à tourner.
2. **Action :**
   - Ouvrez un **troisième terminal**.
   - Lancez l'agrégation : `python stream_processing/agregation.py`
   - Attendez **au moins 30 secondes** (le temps qu'une fenêtre temporelle se termine).
   - Un tableau formaté avec les colonnes Produit, Vues, Panier, Achats, et CA (Chiffre d'Affaires) va s'afficher.
3. **À capturer :** Prenez une belle capture d'écran du terminal affichant ce tableau de statistiques complet.
4. **Nom du fichier :** `agregation_stats.png`

---

## 📸 Capture 3 : Détection d'Abandon de Panier (Alerte Marketing)

**Pourquoi c'est important :** Cela valide l'utilisation de Kafka/Flink pour du "Stateful Processing" (maintien d'un état en mémoire avec des timers). C'est le cas d'usage imposé.

1. **Préparation :**
   - Le simulateur et le script de filtrage doivent tourner.
2. **Action :**
   - Ouvrez un **quatrième terminal**.
   - Lancez la détection : `python stream_processing/detection_abandon.py`
   - Le script va afficher les ajouts au panier ("🛒 PANIER") et démarrer un timer de 60 secondes.
   - Attendez qu'un utilisateur n'achète pas son produit avant la fin du timer.
   - Vous verrez alors un message rouge bien visible : **"🚨 ALERTE ABANDON !"** avec les détails du produit et l'action recommandée.
3. **À capturer :** Prenez une capture d'écran du terminal montrant clairement ce bloc "🚨 ALERTE ABANDON !".
4. **Nom du fichier :** `alerte_abandon.png`

---

## 📸 Capture 4 : Validation de Cohérence (Le Mécanisme de Replay)

**Pourquoi c'est important :** C'est la **preuve absolue** que notre architecture est une architecture Kappa. C'est l'argument technique principal pour la soutenance.

1. **Préparation :**
   - **Très important :** Arrêtez le simulateur (Faites `Ctrl+C` dans le terminal 1).
2. **Action :**
   - Dans n'importe quel terminal libre, lancez le script de replay : `python config/test_replay.py`
   - Le script va tout relire depuis le début de la base de données Kafka, recalculer toutes les statistiques et les comparer avec les données traitées en temps réel.
   - Tout à la fin de l'exécution, un résumé s'affiche sous le titre "🔍 VALIDATION DE COHÉRENCE".
3. **À capturer :** Prenez une capture d'écran de la fin de l'exécution montrant toutes les lignes avec les coches vertes "✅" et la conclusion : **"✅ RÉSULTAT : Le système est COHÉRENT — Replay = Temps Réel"**.
4. **Nom du fichier :** `validation_replay.png`

---

## 📝 Résumé pour l'équipe

| Nom du fichier attendu | Ce qu'il montre |
|------------------------|-----------------|
| `filtrage_bots.png` | Le terminal séparant les bots des humains |
| `agregation_stats.png` | Le tableau des statistiques par produit mis à jour toutes les 30s |
| `alerte_abandon.png` | L'affichage du message "🚨 ALERTE ABANDON !" |
| `validation_replay.png` | Le succès du test de cohérence ("Le système est COHÉRENT") |

Une fois ces images enregistrées dans le dossier `screenshots/`, elles pourront être directement intégrées dans le fichier `rapport_projet gestion/chapters/chapter-04-implementation.tex` de notre rapport !

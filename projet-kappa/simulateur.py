import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Connexion à votre architecture Kafka (sur Docker)
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

actions = ["vue_produit", "ajout_panier", "achat_valide", "abandon_panier"]
produits = ["PC_Gamer", "Souris_Sans_Fil", "Clavier_Mecanique", "Ecran_4K", "Casque_Audio"]

print("🚀 Démarrage du simulateur E-commerce... (Appuyez sur Ctrl+C pour arrêter)")

try:
    while True:
        # Création d'un faux événement client
        evenement = {
            "user_id": f"U{random.randint(1000, 9999)}",
            "action": random.choice(actions),
            "produit": random.choice(produits),
            "timestamp": datetime.now().isoformat()
        }
        
        # Envoi dans le "tuyau" (Topic) Kafka
        producer.send('clics_ecommerce', evenement)
        print(f"➡️ Événement envoyé : {evenement}")
        
        # Pause de 2 secondes avant le prochain client
        time.sleep(2)
except KeyboardInterrupt:
    print("\n🛑 Simulateur arrêté.")
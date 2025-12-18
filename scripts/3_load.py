import pandas as pd
import os

# Chemins
PROCESSED_PATH = "data/processed"
INPUT_FILE = f"{PROCESSED_PATH}/staging_data.csv"
FINAL_FILE = f"{PROCESSED_PATH}/Northwind_Final_Analytics.csv"

def load():
    print("--- 3. Début du Chargement (Load) ---")

    if not os.path.exists(INPUT_FILE):
        print(f"❌ ERREUR : Le fichier {INPUT_FILE} manque.")
        print("👉 As-tu lancé 'python3 scripts/2_transform.py' ?")
        return

    try:
        # Lecture
        df = pd.read_csv(INPUT_FILE)
        
        # Ici, on pourrait ajouter une dernière vérification ou un tri
        df = df.sort_values(by='OrderDate', ascending=False)

        # Sauvegarde Finale
        df.to_csv(FINAL_FILE, index=False)
        
        print(f"🎉 SUCCÈS TOTAL ! L'ETL est terminé.")
        print(f"📊 Fichier final généré : {FINAL_FILE}")
        print(f"ℹ️  Tu peux maintenant lancer le Dashboard : 'streamlit run scripts/dashboard.py'")
        
    except Exception as e:
        print(f"❌ Erreur lors du chargement : {e}")

if __name__ == "__main__":
    load()
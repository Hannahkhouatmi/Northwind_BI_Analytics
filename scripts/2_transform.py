import pandas as pd
import os

# Chemins
RAW_PATH = "data/raw"
PROCESSED_PATH = "data/processed"
INPUT_FILE = f"{RAW_PATH}/raw_merged_data.csv"
OUTPUT_FILE = f"{PROCESSED_PATH}/staging_data.csv"

def transform():
    print("--- 2. Début de la Transformation (Mode Sécurisé) ---")
    
    # 1. Vérification
    if not os.path.exists(INPUT_FILE):
        print(f"❌ ERREUR : {INPUT_FILE} manquant.")
        return

    # 2. Chargement
    df = pd.read_csv(INPUT_FILE)
    count_depart = len(df)
    print(f"📥 Données reçues de l'extraction : {count_depart} lignes.")

    # 3. Conversion Dates
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    df['ShippedDate'] = pd.to_datetime(df['ShippedDate'], errors='coerce')

    # 4. KPI : Livré vs Non Livré
    df['Statut'] = df['ShippedDate'].apply(
        lambda x: 'Non Livré' if pd.isna(x) else 'Livré'
    )

    # 5. GESTION DES MANQUANTS (Au lieu de supprimer)
    # Si le nom du client manque, on met "Client Inconnu" pour ne pas perdre la commande
    df['CompanyName'] = df['CompanyName'].fillna('Client Inconnu')
    
    # Si le nom de l'employé manque
    df['EmployeeName'] = df['EmployeeName'].fillna('Non Assigné')

    # 6. Suppression uniquement si la DATE est vide (car sans date, pas de graphe possible)
    # C'est la seule suppression légitime pour du BI temporel
    df_clean = df.dropna(subset=['OrderDate'])
    
    count_fin = len(df_clean)
    diff = count_depart - count_fin

    # 7. Sauvegarde
    os.makedirs(PROCESSED_PATH, exist_ok=True)
    df_clean.to_csv(OUTPUT_FILE, index=False)
    
    print(f"🧹 Nettoyage : {diff} lignes sans date supprimées.")
    print(f"✅ Transformation terminée.")
    print(f"📊 Lignes conservées : {count_fin} (sur {count_depart})")

if __name__ == "__main__":
    transform()


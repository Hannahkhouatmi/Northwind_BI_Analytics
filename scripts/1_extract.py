import pandas as pd
import pyodbc
import os
import glob
import warnings

warnings.filterwarnings('ignore')

# --- CONFIG ---
RAW_PATH = "data/raw"
EXTERNAL_PATH = "data/external_files"
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=Northwind;"
    "UID=sa;PWD=M1crosoft_2025!;TrustServerCertificate=yes;"
)

def clean_id(val):
    """Nettoie les IDs (enlève les .0 et les espaces)"""
    return str(val).replace('.0', '').strip()

def extract_clean():
    print("\n🚀 DÉMARRAGE : Extraction Ciblée (SQL + Orders.xlsx)")
    os.makedirs(RAW_PATH, exist_ok=True)
    
    all_orders = []

    #
    # 1. RÉCUPÉRATION DES RÉFÉRENTIELS (SQL)
    
    try:
        conn = pyodbc.connect(CONN_STR)
        
        # On récupère les Clients
        ref_customers = pd.read_sql("SELECT CustomerID, CompanyName FROM Customers", conn)
        ref_customers['CustomerID'] = ref_customers['CustomerID'].apply(clean_id)
        
        # On récupère les Employés
        ref_employees = pd.read_sql("SELECT EmployeeID, FirstName + ' ' + LastName as Name FROM Employees", conn)
        ref_employees['EmployeeID'] = ref_employees['EmployeeID'].apply(clean_id)
        
        print("✅ Référentiels Clients & Employés chargés.")
    except Exception as e:
        print(f"❌ Erreur Connexion SQL (Référentiels): {e}")
        return

    # ==========================================
    # 2. SQL SERVER (Historique)
    # ==========================================
    try:
        query = """
        SELECT o.OrderID, o.CustomerID, c.CompanyName, o.EmployeeID, 
               e.FirstName + ' ' + e.LastName as EmployeeName, o.OrderDate, o.ShippedDate
        FROM Orders o
        LEFT JOIN Customers c ON o.CustomerID = c.CustomerID
        LEFT JOIN Employees e ON o.EmployeeID = e.EmployeeID
        """
        df_sql = pd.read_sql(query, conn)
        conn.close()
        
        df_sql['Source'] = 'SQL_Server'
        df_sql['OrderID'] = df_sql['OrderID'].apply(clean_id)
        
        all_orders.append(df_sql)
        print(f"✅ SQL Server : {len(df_sql)} commandes historiques.")
    except Exception as e:
        print(f"❌ Erreur SQL Data : {e}")

    # ==========================================
    # 3. EXCEL : ORDERS.XLSX (Commandes Récentes)
    # ==========================================
    ord_file = os.path.join(EXTERNAL_PATH, "Orders.xlsx")
    
    if os.path.exists(ord_file):
        try:
            df_xl = pd.read_excel(ord_file)
            print(f"📄 Lecture de {os.path.basename(ord_file)}...")

            rename_map = {
                'Order ID': 'OrderID', 
                'Customer': 'CustomerID',     
                'Customer ID': 'CustomerID',   
                'Employee': 'EmployeeID',      
                'Employee ID': 'EmployeeID',
                'Order Date': 'OrderDate', 
                'Shipped Date': 'ShippedDate',
                'Ship Name': 'CompanyName_Backup'
            }
            df_xl.rename(columns=rename_map, inplace=True)
            
            # Vérification de sécurité
            if 'CustomerID' not in df_xl.columns:
                print(f"❌ ERREUR : Colonne 'Customer' ou 'CustomerID' introuvable dans Excel.")
                print(f"Colonnes dispos : {list(df_xl.columns)}")
                return

            # Nettoyage ID pour la jointure
            df_xl['CustomerID'] = df_xl['CustomerID'].apply(clean_id)
            
            if 'EmployeeID' in df_xl.columns:
                df_xl['EmployeeID'] = df_xl['EmployeeID'].apply(clean_id)
            else:
                df_xl['EmployeeID'] = None # Si pas de colonne employé, on crée une vide

            df_xl['OrderID'] = df_xl['OrderID'].apply(clean_id)

            # --- JOINTURES POUR AVOIR LES NOMS ---
            # 1. Récupérer le nom du Client via son ID
            df_xl = df_xl.merge(ref_customers, on='CustomerID', how='left')
            
            # Sécurité : Si le nom est vide après le merge, on essaie le Ship Name
            if 'CompanyName_Backup' in df_xl.columns:
                df_xl['CompanyName'] = df_xl['CompanyName'].fillna(df_xl['CompanyName_Backup'])

            # 2. Récupérer le nom de l'Employé
            df_xl = df_xl.merge(ref_employees, on='EmployeeID', how='left')
            if 'Name' in df_xl.columns:
                df_xl.rename(columns={'Name': 'EmployeeName'}, inplace=True)
            else:
                df_xl['EmployeeName'] = 'Inconnu'

            df_xl['Source'] = 'Excel_Orders'
            
            all_orders.append(df_xl)
            print(f"✅ Orders.xlsx : {len(df_xl)} commandes ajoutées.")
            
        except Exception as e:
            print(f"❌ Erreur lecture Orders.xlsx : {e}")
    else:
        print(f"⚠️ ATTENTION : Le fichier {ord_file} est introuvable !")


    # 4. FUSION FINALE

    if all_orders:
        df_final = pd.concat(all_orders, ignore_index=True)
        
        # Sélection des colonnes propres
        cols = ['OrderID', 'CustomerID', 'CompanyName', 'EmployeeID', 'EmployeeName', 'OrderDate', 'ShippedDate', 'Source']
        
        # Création des colonnes manquantes si besoin
        for c in cols:
            if c not in df_final.columns:
                df_final[c] = None
        
        df_final = df_final[cols]

        # Suppression doublons (Priorité SQL si conflit)
        df_final.drop_duplicates(subset=['OrderID'], keep='first', inplace=True)
        
        output = f"{RAW_PATH}/raw_merged_data.csv"
        df_final.to_csv(output, index=False)
        
        print(f"\n🎉 SUCCÈS : {len(df_final)} Commandes au total.")
        print(f"📊 Détail :")
        print(df_final['Source'].value_counts())
    else:
        print("❌ Aucune donnée récupérée.")

if __name__ == "__main__":
    extract_clean()
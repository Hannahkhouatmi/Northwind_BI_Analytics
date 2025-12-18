# import pandas as pd
# import pyodbc
# import os
# import glob
# import warnings

# warnings.filterwarnings('ignore')

# # --- CONFIG ---
# RAW_PATH = "data/raw"
# EXTERNAL_PATH = "data/external_files"
# CONN_STR = (
#     "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=Northwind;"
#     "UID=sa;PWD=M1crosoft_2025!;TrustServerCertificate=yes;"
# )

# def clean_id(val):
#     """Uniformise les ID en texte sans .0"""
#     return str(val).replace('.0', '').strip()

# def extract_ultimate():
#     print("\n🚀 DÉMARRAGE : Recherche des 878 commandes...")
#     os.makedirs(RAW_PATH, exist_ok=True)
    
#     all_orders = []

#     # ==========================================
#     # 1. SQL SERVER (Base : ~830 commandes)
#     # ==========================================
#     try:
#         conn = pyodbc.connect(CONN_STR)
#         query = """
#         SELECT o.OrderID, o.CustomerID, c.CompanyName, o.EmployeeID, 
#                e.FirstName + ' ' + e.LastName as EmployeeName, o.OrderDate, o.ShippedDate
#         FROM Orders o
#         LEFT JOIN Customers c ON o.CustomerID = c.CustomerID
#         LEFT JOIN Employees e ON o.EmployeeID = e.EmployeeID
#         """
#         df_sql = pd.read_sql(query, conn)
#         df_sql['Source'] = 'SQL_Server'
        
#         # Récupération référentiels pour la suite
#         ref_suppliers = pd.read_sql("SELECT SupplierID, CompanyName FROM Suppliers", conn)
#         ref_suppliers['SupplierID'] = ref_suppliers['SupplierID'].apply(clean_id)
        
#         ref_employees = pd.read_sql("SELECT EmployeeID, FirstName + ' ' + LastName as Name FROM Employees", conn)
#         ref_employees['EmployeeID'] = ref_employees['EmployeeID'].apply(clean_id)
        
#         conn.close()
#         all_orders.append(df_sql)
#         print(f"✅ SQL Server : {len(df_sql)} commandes.")
#     except Exception as e:
#         print(f"❌ Erreur SQL : {e}")

#     # ==========================================
#     # 2. EXCEL : PURCHASE ORDERS (Année 2006 : ~28 commandes)
#     # ==========================================
#     po_file = f"{EXTERNAL_PATH}/Purchase Orders.xlsx"
#     if os.path.exists(po_file):
#         try:
#             df_po = pd.read_excel(po_file)
#             # Mapping spécifique
#             df_po.rename(columns={
#                 'Purchase Order ID': 'OrderID', 'Supplier ID': 'CustomerID', 
#                 'Creation Date': 'OrderDate', 'Submitted Date': 'ShippedDate', 'Created By': 'EmployeeID'
#             }, inplace=True)
            
#             # Enrichissement Noms
#             df_po['CustomerID'] = df_po['CustomerID'].apply(clean_id)
#             df_po['EmployeeID'] = df_po['EmployeeID'].apply(clean_id)
            
#             # Nom Client (via Fournisseur)
#             df_po = df_po.merge(ref_suppliers, left_on='CustomerID', right_on='SupplierID', how='left')
#             # Nom Employé
#             df_po = df_po.merge(ref_employees, on='EmployeeID', how='left')
#             df_po.rename(columns={'Name': 'EmployeeName'}, inplace=True)
            
#             df_po['Source'] = 'Excel_Purchase'
#             all_orders.append(df_po)
#             print(f"✅ Purchase Orders : {len(df_po)} commandes trouvées.")
#         except Exception as e:
#             print(f"❌ Erreur Purchase Orders : {e}")

#     # ==========================================
#     # 3. EXCEL : ORDERS.XLSX (Potentielles commandes manquantes)
#     # ==========================================
#     # On force la lecture même si on a déjà SQL, car il peut y avoir des "perdues"
#     ord_file = f"{EXTERNAL_PATH}/Orders.xlsx"
#     if os.path.exists(ord_file):
#         try:
#             df_o = pd.read_excel(ord_file)
#             rename = {'Order ID': 'OrderID', 'Customer ID': 'CustomerID', 'Employee ID': 'EmployeeID',
#                       'Order Date': 'OrderDate', 'Shipped Date': 'ShippedDate', 'Ship Name': 'CompanyName'}
#             df_o.rename(columns=rename, inplace=True)
#             df_o['Source'] = 'Excel_Orders_File'
            
#             # Nettoyage ID pour comparaison
#             df_o['OrderID'] = df_o['OrderID'].apply(clean_id)
            
#             all_orders.append(df_o)
#             print(f"✅ Orders.xlsx : {len(df_o)} commandes lues (sera dédoublonné).")
#         except:
#             pass

#     # ==========================================
#     # 4. EXCEL : INVOICES.XLSX (Le dernier espoir des 20 manquantes)
#     # ==========================================
#     inv_file = f"{EXTERNAL_PATH}/Invoices.xlsx"
#     if os.path.exists(inv_file):
#         try:
#             df_inv = pd.read_excel(inv_file)
#             # Invoices a souvent Order ID aussi
#             rename = {'Order ID': 'OrderID', 'Customer ID': 'CustomerID', 'Order Date': 'OrderDate', 
#                       'Shipped Date': 'ShippedDate', 'Customer Name': 'CompanyName'}
#             df_inv.rename(columns=rename, inplace=True)
#             df_inv['Source'] = 'Excel_Invoices'
#             df_inv['OrderID'] = df_inv['OrderID'].apply(clean_id)
            
#             all_orders.append(df_inv)
#             print(f"✅ Invoices.xlsx : {len(df_inv)} factures lues (sera dédoublonné).")
#         except:
#             pass

#     # ==========================================
#     # 5. FUSION ET DÉDOUBLONNAGE INTELLIGENT
#     # ==========================================
#     if all_orders:
#         # On empile tout
#         df_final = pd.concat(all_orders, ignore_index=True)
        
#         # On nettoie les OrderID pour être sûr que "10248" == 10248
#         df_final['OrderID'] = df_final['OrderID'].apply(clean_id)
        
#         # Colonnes finales souhaitées
#         cols = ['OrderID', 'CustomerID', 'CompanyName', 'EmployeeID', 'EmployeeName', 'OrderDate', 'ShippedDate', 'Source']
#         for c in cols:
#             if c not in df_final.columns: df_final[c] = None
            
#         df_final = df_final[cols]

#         # --- LE SECRET EST ICI ---
#         # On supprime les doublons en gardant la priorité :
#         # 1. Purchase Orders (2006)
#         # 2. SQL Server (Base sure)
#         # 3. Excel Files (Compléments)
        
#         # On trie pour mettre SQL et Purchase en premier (pour qu'ils soient gardés par 'keep=first')
#         df_final['Source_Rank'] = df_final['Source'].map({
#             'Excel_Purchase': 1, 
#             'SQL_Server': 2, 
#             'Excel_Orders_File': 3,
#             'Excel_Invoices': 4
#         })
#         df_final.sort_values(by=['Source_Rank'], inplace=True)
        
#         # Suppression des doublons basés sur OrderID
#         before = len(df_final)
#         df_final.drop_duplicates(subset=['OrderID'], keep='first', inplace=True)
#         after = len(df_final)
        
#         # Sauvegarde
#         output = f"{RAW_PATH}/raw_merged_data.csv"
#         df_final.to_csv(output, index=False)
        
#         print(f"\n✨ NETTOYAGE : {before - after} doublons retirés.")
#         print(f"🏆 TOTAL FINAL : {len(df_final)} Commandes Uniques.")
#         print(f"   (SQL + Purchase + Potentiels ajouts Orders/Invoices)")
        
#         # Petit check pour toi
#         print("\n📊 Répartition finale par Source :")
#         print(df_final['Source'].value_counts())

# if __name__ == "__main__":
#     extract_ultimate()


# import pandas as pd
# import pyodbc
# import os
# import glob
# import warnings

# warnings.filterwarnings('ignore') # On cache les warnings pandas inutiles

# # --- CONFIG ---
# RAW_PATH = "data/raw"
# EXTERNAL_PATH = "data/external_files"
# CONN_STR = (
#     "DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=Northwind;"
#     "UID=sa;PWD=M1crosoft_2025!;TrustServerCertificate=yes;"
# )

# def force_to_string(df, col):
#     """Force une colonne en string pour éviter les erreurs de merge (int vs float vs str)"""
#     if col in df.columns:
#         df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True) # Enlève le ".0" des floats excel
#     return df

# def extract_debug_mode():
#     print("\n🔍 --- DÉBUT DIAGNOSTIC & EXTRACTION ---")
#     os.makedirs(RAW_PATH, exist_ok=True)
    
#     transactions = []

#     # 1. SQL SERVER
#     try:
#         conn = pyodbc.connect(CONN_STR)
#         query = """
#         SELECT o.OrderID, o.CustomerID, c.CompanyName, o.EmployeeID, 
#                e.FirstName + ' ' + e.LastName as EmployeeName, o.OrderDate, o.ShippedDate
#         FROM Orders o
#         LEFT JOIN Customers c ON o.CustomerID = c.CustomerID
#         LEFT JOIN Employees e ON o.EmployeeID = e.EmployeeID
#         """
#         df_sql = pd.read_sql(query, conn)
#         df_sql['Source'] = 'SQL_Server'
#         # On convertit tout en string pour uniformiser
#         df_sql = force_to_string(df_sql, 'CustomerID')
#         df_sql = force_to_string(df_sql, 'EmployeeID')
        
#         # Récupération Référentiels SQL pour Excel
#         ref_suppliers = pd.read_sql("SELECT SupplierID, CompanyName FROM Suppliers", conn)
#         ref_suppliers = force_to_string(ref_suppliers, 'SupplierID')
        
#         ref_employees = pd.read_sql("SELECT EmployeeID, FirstName + ' ' + LastName as Name FROM Employees", conn)
#         ref_employees = force_to_string(ref_employees, 'EmployeeID')
        
#         conn.close()
#         transactions.append(df_sql)
#         print(f"✅ SQL Server : {len(df_sql)} lignes récupérées.")
#     except Exception as e:
#         print(f"❌ ERREUR SQL : {e}")
#         return # Si SQL plante, on arrête tout, vérifie Docker !

#     # 2. EXCEL : LECTURE DE TOUS LES FICHIERS EXCEL
#     excel_files = [
#             f for f in glob.glob(f"{EXTERNAL_PATH}/*.xlsx")
#             if not os.path.basename(f).startswith("~$")
#         ]

#     print(f"📂 {len(excel_files)} fichiers Excel détectés dans le dossier.")

#     # === 1. DÉFINITION DES FICHIERS QU'ON VEUT VRAIMENT ===
#     # On ne met QUE les fichiers principaux de commandes ici.
#     # On ignore "Strings.xlsx", "Privileges.xlsx", les fichiers "Status", etc.
#     TARGET_FILES = ['Orders.xlsx', 'Purchase Orders.xlsx'] 

#     for file in excel_files:
#         # On récupère le nom du fichier (ex: "Orders.xlsx")
#         filename = os.path.basename(file)

#         # === 2. LE FILTRE DE SÉCURITÉ ===
#         if filename not in TARGET_FILES:
#             # Si le fichier n'est pas dans notre liste, on l'ignore
#             # print(f"   🚫 Ignoré : {filename} (Pas pertinent)") 
#             continue  # "continue" force la boucle à passer au fichier suivant direct
        
#         # Si on arrive ici, c'est que c'est un bon fichier !
#         try:
#             print(f"\n📄 Lecture du fichier VALIDÉ : {filename}")
#             df_xl = pd.read_excel(file)

#             print(f"   ➜ Colonnes détectées : {list(df_xl.columns)}")

#             rename_map = {
#                 'Order ID': 'OrderID',
#                 'Purchase Order ID': 'OrderID',
#                 'Customer ID': 'CustomerID',
#                 'Supplier ID': 'CustomerID',
#                 'Employee ID': 'EmployeeID',
#                 'Created By': 'EmployeeID',
#                 'Order Date': 'OrderDate',
#                 'Creation Date': 'OrderDate',
#                 'Shipped Date': 'ShippedDate',
#                 'Submitted Date': 'ShippedDate',
#                 'Ship Name': 'CompanyName'
#             }
#             df_xl.rename(columns=rename_map, inplace=True)

#             # ... LE RESTE DE TON CODE RESTE PAREIL ...
#             df_xl = force_to_string(df_xl, 'CustomerID')
#             df_xl = force_to_string(df_xl, 'EmployeeID')

#             if 'CustomerID' in df_xl.columns:
#                 df_xl = df_xl.merge(
#                     ref_suppliers,
#                     left_on='CustomerID',
#                     right_on='SupplierID',
#                     how='left'
#                 )

#             if 'EmployeeID' in df_xl.columns:
#                 df_xl = df_xl.merge(
#                     ref_employees,
#                     on='EmployeeID',
#                     how='left'
#                 )
#                 df_xl.rename(columns={'Name': 'EmployeeName'}, inplace=True)

#             source_name = os.path.splitext(os.path.basename(file))[0]
#             df_xl['Source'] = f"Excel_{source_name}"

#             required_cols = [
#                 'OrderID', 'CustomerID', 'CompanyName',
#                 'EmployeeID', 'EmployeeName',
#                 'OrderDate', 'ShippedDate', 'Source'
#             ]

#             for col in required_cols:
#                 if col not in df_xl.columns:
#                     df_xl[col] = None

#             final_xl = df_xl[required_cols]

#             # 🔥 FIX CRITIQUE
#             final_xl = final_xl.loc[:, ~final_xl.columns.duplicated()]

#             transactions.append(final_xl)

#             print(f"   ✅ {len(final_xl)} lignes ajoutées depuis {source_name}")

#         except Exception as e:
#             print(f"   ❌ Erreur avec {file} : {e}")



#     # 4. FUSION
#     if len(transactions) > 0:
#         transactions = [
#                 df.loc[:, ~df.columns.duplicated()]
#                 for df in transactions
#     ]
#         df_final = pd.concat(transactions, ignore_index=True)
#         # Supprime doublons d'ID commande si conflit SQL/Excel
#         df_final.drop_duplicates(subset=['OrderID', 'Source'], inplace=True)
        
#         output = f"{RAW_PATH}/raw_merged_data.csv"
#         df_final.to_csv(output, index=False)
#         print(f"\n🎉 RÉSULTAT FINAL : {len(df_final)} lignes sauvegardées dans {output}")
#         print("👉 Lance maintenant : python3 scripts/2_transform.py")
#     else:
#         print("❌ AUCUNE DONNÉE RÉCUPÉRÉE.")

# if __name__ == "__main__":
#     extract_debug_mode()

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

    # ==========================================
    # 1. RÉCUPÉRATION DES RÉFÉRENTIELS (SQL)
    # ==========================================
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

            # --- CORRECTION ICI : "Customer" -> "CustomerID" ---
            rename_map = {
                'Order ID': 'OrderID', 
                'Customer': 'CustomerID',      # <--- C'est ici que ça bloquait !
                'Customer ID': 'CustomerID',   # On garde l'ancien au cas où
                'Employee': 'EmployeeID',      # On prévoit aussi celui-là
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
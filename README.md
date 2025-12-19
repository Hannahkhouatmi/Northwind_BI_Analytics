# Northwind_BI_Analytics
📊 Présentation du Projet
Ce projet consiste en la conception et la réalisation d'une solution de Business Intelligence (BI) de bout en bout. L'objectif est de transformer les données brutes de la célèbre base de données Northwind en informations exploitables pour la prise de décision stratégique.

La solution intègre un pipeline ETL complet développé en Python, une architecture modulaire et un tableau de bord interactif pour l'analyse multidimensionnelle.


🛠️ Stack Technique

Base de données : SQL Server (déployé via Docker).

Langage : Python 3.x (Pandas pour la manipulation de données).

Visualisation : Streamlit & Plotly (Analyse 2D et 3D).

Outils tiers : DBeaver (Exploration SQL).

🏗️ Architecture du Projet
L'arborescence du projet respecte une logique de séparation des préoccupations pour garantir la scalabilité et la traçabilité:

Plaintext

/data
  ├── /raw            # Données brutes extraites (SQL & Excel)
  ├── /processed      # Données nettoyées (Staging Area)
  └── /external_files # Exports finaux pour l'analyse
/scripts
  ├── 1_extract.py    # Extraction SQL vers CSV
  ├── 2_transform.py  # Nettoyage et enrichissement (Logique BI)
  ├── 3_load.py       # Chargement et tri final
  └── dashboard.py    # Application Streamlit
/reports              # Documentation et rapports de projet
/figures              # Captures des visualisations
README.md             # Documentation technique


⚙️ Pipeline ETL
Le processus ETL (Extract, Transform, Load) est le cœur du projet :

Extraction : Consolidation de ~830 commandes historiques depuis SQL Server et de commandes récentes via Excel, avec dédoublonnage intelligent.


Transformation : * Conversion des formats de date.

Création d'un KPI métier (Statut de livraison : Livré / Non Livré).
Gestion des valeurs manquantes (ex: "Client Inconnu" plutôt que suppression) pour préserver l'intégrité analytique.

Chargement : Préparation du dataset final Northwind_Final_Analytics.csv trié par date.

📈 Dashboard & Analyse
L'interface Streamlit permet une exploration multidimensionnelle :


KPIs en temps réel : Volume total, taux de livraison, et dernière activité.

Analyses Temporelles : Évolution mensuelle des commandes.
Analyse de Performance : Top 10 clients et performance par employé.
Analyse OLAP 3D : Une visualisation innovante utilisant Plotly pour croiser simultanément les Dates, les Clients et les Employés.

🚀 Installation et Utilisation

Lancer la base de données : Utiliser Docker pour déployer l'instance SQL Server contenant Northwind.

Installer les dépendances : ```bash pip install pandas streamlit plotly sqlalchemy pyodbc

Exécuter le pipeline :

Bash

python scripts/1_extract.py
python scripts/2_transform.py
python scripts/3_load.py
Lancer le Dashboard :

Bash

streamlit run scripts/dashboard.py
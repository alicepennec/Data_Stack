import streamlit as st
import pandas as pd
import requests
from sqlalchemy import create_engine
from ydata_profiling import ProfileReport
from streamlit.components.v1 import html

# Configuration globale
st.set_page_config(page_title="DataStack - Data Engineering App", layout="wide")

# === Fonctionnalités Générales ===

def load_local_file(file, delimiter):
    """Charger un fichier local (CSV, Excel, etc.) avec un délimiteur défini."""
    try:
        if file.name.endswith(".csv"):
            return pd.read_csv(file, delimiter=delimiter)
        elif file.name.endswith(".xlsx"):
            return pd.read_excel(file)
        else:
            st.error("Type de fichier non supporté. Veuillez utiliser un fichier CSV ou Excel.")
            return None
    except Exception as e:
        st.error(f"Erreur lors du chargement du fichier : {e}")
        return None

def load_from_database(connection_string, query):
    """Charger des données à partir d'une base de données via SQLAlchemy."""
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données : {e}")
        return None

def load_from_api(api_url, headers, params):
    """Charger des données depuis une API."""
    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()  # Lève une exception pour les erreurs HTTP
        data = response.json()  # Décoder la réponse JSON
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict) and 'data' in data:
            return pd.DataFrame(data['data'])
        else:
            st.error("Format des données API non supporté. Attendu : liste ou dictionnaire avec clé 'data'.")
            return None
    except Exception as e:
        st.error(f"Erreur lors de la connexion à l'API : {e}")
        return None

def clean_data(df):
    """Nettoyer les données (exemple simplifié)."""
    df = df.dropna()  # Supprime les valeurs manquantes
    df = df.drop_duplicates()  # Supprime les doublons
    return df

def explore_data(df):
    """Effectuer une EDA simple."""
    st.write("**Résumé statistique**")
    st.write(df.describe())

def generate_profile_report(df):
    """Générer un rapport de profilage interactif."""
    profile = ProfileReport(df, title="Rapport EDA", explorative=True)
    profile.to_file("eda_report.html")
    with open("eda_report.html", "r", encoding="utf-8") as f:
        report_html = f.read()
    html(report_html, height=1000, scrolling=True)

# === Interface Utilisateur ===
st.title("🛠️ DataStack - Plateforme de Data Engineering")

# 1. Sélection de la source de données
st.sidebar.header("1️⃣ Charger les données")
source_type = st.sidebar.radio(
    "Choisissez la source de données",
    options=["Fichier local", "Base de données", "API"]
)

data = None

if source_type == "Fichier local":
    uploaded_file = st.sidebar.file_uploader("Téléversez votre fichier", type=["csv", "xlsx"])
    delimiter = st.sidebar.text_input("Délimiteur (par défaut : ',')", value=",")
    if uploaded_file is not None:
        data = load_local_file(uploaded_file, delimiter)

elif source_type == "Base de données":
    db_connection = st.sidebar.text_input("Chaîne de connexion (SQLAlchemy)", "")
    db_query = st.sidebar.text_area("Requête SQL", "SELECT * FROM your_table")
    if st.sidebar.button("Charger depuis la base de données"):
        data = load_from_database(db_connection, db_query)

elif source_type == "API":
    api_url = st.sidebar.text_input("URL de l'API", "https://api.example.com/data")
    headers_input = st.sidebar.text_area("En-têtes (format JSON)", '{"Authorization": "Bearer YOUR_TOKEN"}')
    params_input = st.sidebar.text_area("Paramètres (format JSON)", '{"key1": "value1", "key2": "value2"}')

    # Convertir les entrées texte en dictionnaires
    headers = {}
    params = {}
    try:
        if headers_input:
            headers = eval(headers_input)  # Convertir la chaîne JSON en dictionnaire
        if params_input:
            params = eval(params_input)
    except Exception as e:
        st.error(f"Erreur dans le format des en-têtes ou des paramètres : {e}")

    if st.sidebar.button("Charger depuis l'API"):
        data = load_from_api(api_url, headers, params)

# 2. Traiter les données
if data is not None:
    st.sidebar.header("2️⃣ Traiter les données")
    action = st.sidebar.selectbox(
        "Choisissez une action",
        options=["Aperçu des données", "Nettoyage des données", "EDA (Exploration des données)"]
    )
    
    # Aperçu des données
    if action == "Aperçu des données":
        st.subheader("🔍 Aperçu des données")
        st.dataframe(data.head())  # Afficher un aperçu des données
    
    # Nettoyage des données
    elif action == "Nettoyage des données":
        st.subheader("🧹 Nettoyage des données")
        cleaned_data = clean_data(data)  # Nettoyage des données
        st.dataframe(cleaned_data)  # Afficher les données nettoyées
        
        # Télécharger les données nettoyées
        st.download_button(
            "Télécharger les données nettoyées",
            data=cleaned_data.to_csv(index=False),
            file_name="cleaned_data.csv"
        )
        
    # Exploration des données
    elif action == "EDA (Exploration des données)":
        st.subheader("📊 Exploration des données")
        st.sidebar.info("Un peu de patience...")
        if st.sidebar.button("Générer un rapport de profilage interactif"):
            with st.spinner("Génération du rapport..."):
                generate_profile_report(data)


# 3. Construction de pipeline ETL
if data is not None:
    st.sidebar.header("3️⃣ Conception BDD")
    steps = st.sidebar.selectbox(
        "Conception BDD",
        options=["Aucune action", "Création tables", "Création contrainte", "Génération schéma"],
        index=0
    )
    
    # Transformation des données (Création de tables de faits et dimensions)
    if steps == "Création tables":
        st.subheader("🔄 Création tables de faits et dimensions")
    
        # Vérifier si les données ont été nettoyées
        if 'cleaned_data' in locals():
            transformation_data = cleaned_data  # Utiliser les données nettoyées
        else:
            transformation_data = data
            st.warning("⚠️ Les données brutes seront utilisées car aucune étape de nettoyage n'a été effectuée.")
        
        # Création de la table de faits
        st.write("#### Sélectionnez les colonnes pour la Table de Faits")
        fact_columns = st.multiselect(
            "Colonnes pour la Table de Faits",
            options=transformation_data.columns
        )
    
        # Vérifier si des colonnes pour la table de faits ont été sélectionnées
        if fact_columns:
            fact_table = transformation_data[fact_columns]
            fact_table.insert(0, 'ID', range(1, len(fact_table) + 1))  # Ajouter une colonne ID
            st.write("#### Table de Faits")
            st.dataframe(fact_table.head())
            
            # Bouton pour télécharger la table de faits
            st.download_button(
                label="Télécharger la Table de Faits",
                data=fact_table.to_csv(index=False),
                file_name="fact_table.csv",
                mime="text/csv"
            )
        else:
            st.warning("Veuillez sélectionner des colonnes pour la Table de Faits.")
    
        # Interface pour définir plusieurs tables de dimensions
        st.write("#### Définir des Tables de Dimensions")
        num_dimensions = st.number_input(
            "Combien de tables de dimensions voulez-vous créer ?",
            min_value=1,
            max_value=10,
            value=1,
            step=1
        )
    
        dimension_tables = []  # Liste pour stocker les tables de dimensions
        dimension_names = []   # Liste pour stocker les noms des tables

        for i in range(num_dimensions):
            st.write(f"##### Table de Dimensions {i + 1}")
            
            # Saisir un nom pour la table
            dimension_name = st.text_input(f"Nom pour la Table de Dimensions {i + 1}", value=f"Dimension_{i + 1}")
            dimension_names.append(dimension_name)

            # Sélection des colonnes pour cette table
            dimension_columns = st.multiselect(
                f"Colonnes pour la Table {dimension_name}",
                options=transformation_data.columns,
                key=f"dim_columns_{i}"  # Clé unique pour chaque widget
            )
        
            if dimension_columns:
                # Créer la table de dimensions
                dimension_table = transformation_data[dimension_columns]
                dimension_table.insert(0, f"{dimension_name}ID", range(1, len(dimension_table) + 1))  # Ajouter une colonne ID
                dimension_tables.append(dimension_table)
                
                st.write(f"Table de Dimensions : **{dimension_name}**")
                st.dataframe(dimension_table.head())
                
                # Bouton pour télécharger la table
                st.download_button(
                    label=f"Télécharger la Table {dimension_name}",
                    data=dimension_table.to_csv(index=False),
                    file_name=f"{dimension_name}.csv",
                    mime="text/csv"
                )
            else:
                st.warning(f"Veuillez sélectionner des colonnes pour la Table de Dimensions {i + 1}.")

    
    if st.sidebar.button("Exécuter"):
        st.subheader("⚙️ Conception BDD")
        st.write(f"Étapes sélectionnées : {steps}")
        if "Création tables" in steps:
            st.write("✔️ Tables créées.")
        if "Création contrainte" in steps:
            st.write("✔️ Contraintes créées.")
        if "Génération schéma" in steps:
            st.write("✔️ Schéma généré.")
import streamlit as st
import pandas as pd
<<<<<<< HEAD
import requests
from sqlalchemy import create_engine
=======
import numpy as np 
import requests
from sqlalchemy import create_engine
from ydata_profiling import ProfileReport
from streamlit.components.v1 import html
>>>>>>> b2a292889ae145893c8aad36652f871109564ab8

# Configuration globale
st.set_page_config(page_title="DataStack - Data Engineering App", layout="wide")

<<<<<<< HEAD
=======

>>>>>>> b2a292889ae145893c8aad36652f871109564ab8
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
    st.write("**Aperçu des données**")
    st.dataframe(df.head())
    st.write("**Résumé statistique**")
    st.write(df.describe())

<<<<<<< HEAD
=======
def generate_profile_report(df):
    """Générer un rapport de profilage interactif."""
    profile = ProfileReport(df, title="Rapport EDA", explorative=True)
    profile.to_file("eda_report.html")
    with open("eda_report.html", "r", encoding="utf-8") as f:
        report_html = f.read()
    html(report_html, height=1000, scrolling=True)

>>>>>>> b2a292889ae145893c8aad36652f871109564ab8
# === Interface Utilisateur ===
st.title("🛠️ DataStack - Plateforme de Data Engineering")

# 1. Sélection de la source de données
st.sidebar.header("1️⃣ Charger les données")
source_type = st.sidebar.radio(
    "Choisissez la source de données",
    options=["Fichier local", "Base de données", "API"]
)

<<<<<<< HEAD
=======
data = None

>>>>>>> b2a292889ae145893c8aad36652f871109564ab8
if source_type == "Fichier local":
    uploaded_file = st.sidebar.file_uploader("Téléversez votre fichier", type=["csv", "xlsx"])
    delimiter = st.sidebar.text_input("Délimiteur (par défaut : ',')", value=";")
    if uploaded_file is not None:
        data = load_local_file(uploaded_file, delimiter)
        if data is not None:
            st.success("Données chargées depuis le fichier local.")
            st.write("Aperçu des données :")
            st.dataframe(data.head())

elif source_type == "Base de données":
    db_connection = st.sidebar.text_input("Chaîne de connexion (SQLAlchemy)", "")
    db_query = st.sidebar.text_area("Requête SQL", "SELECT * FROM your_table")
    if st.sidebar.button("Charger depuis la base de données"):
        data = load_from_database(db_connection, db_query)
        if data is not None:
            st.success("Données chargées depuis la base de données.")
            st.write("Aperçu des données :")
            st.dataframe(data.head())

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
        if data is not None:
            st.success("Données chargées depuis l'API.")
            st.write("Aperçu des données :")
            st.dataframe(data.head())

# 2. Nettoyage et exploration
<<<<<<< HEAD
if "data" in locals() and data is not None:
=======
if data is not None:
>>>>>>> b2a292889ae145893c8aad36652f871109564ab8
    st.sidebar.header("2️⃣ Traiter les données")
    action = st.sidebar.selectbox(
        "Choisissez une action",
        options=["Aperçu des données", "Nettoyer les données", "EDA (Exploration des Données)"]
    )
    
    if action == "Aperçu des données":
        st.subheader("🔍 Aperçu des données")
        st.dataframe(data)
    elif action == "Nettoyer les données":
        st.subheader("🧹 Nettoyage des données")
        cleaned_data = clean_data(data)
        st.dataframe(cleaned_data)
        st.download_button("Télécharger les données nettoyées", data=cleaned_data.to_csv(index=False), file_name="cleaned_data.csv")
    elif action == "EDA (Exploration des Données)":
        st.subheader("📊 Exploration des Données")
<<<<<<< HEAD
        explore_data(data)

# 3. Construction de pipeline ETL
st.sidebar.header("3️⃣ Pipeline ETL")
if "data" in locals() and data is not None:
    etl_step = st.sidebar.multiselect(
        "Étapes du pipeline ETL",
        options=["Extraction", "Transformation", "Chargement"]
    )
    
    if st.sidebar.button("Exécuter le Pipeline ETL"):
        st.subheader("⚙️ Pipeline ETL")
        st.write(f"Étapes sélectionnées : {etl_step}")
        # Exemple d'exécution (modifiez selon vos besoins)
        if "Extraction" in etl_step:
            st.write("✔️ Données extraites.")
        if "Transformation" in etl_step:
            transformed_data = clean_data(data)
            st.write("✔️ Données transformées.")
        if "Chargement" in etl_step:
            st.write("✔️ Données chargées dans la destination (non implémenté).")

st.sidebar.info("Application évolutive - Ajoutez vos propres modules pour enrichir cette plateforme.")
=======
        st.sidebar.info("Cela peut prendre un moment selon la taille de votre jeu de données.")
        if st.sidebar.button("Générer un rapport de profilage interactif"):
            with st.spinner("Génération du rapport..."):
                generate_profile_report(data)
>>>>>>> b2a292889ae145893c8aad36652f871109564ab8

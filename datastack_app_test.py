import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import requests

# Configuration globale
st.set_page_config(page_title="DataStack - Data Engineering App", layout="wide")

# === Fonctionnalités Générales ===
#TODOtodo ajouter l'encoding

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
    
    
def load_from_api(API_URL):
    """Charger des données à partir d'une API."""

     
    try:  
        response = requests.get(API_URL)  
        res=response.raise_for_status()  # Vérifie les erreurs  
        if res == 200:
            data = response.json()  
            return data  
    except requests.exceptions.RequestException as e:  
        st.error(f"Erreur lors de la requête: {e}")  
        return []  

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
def transform():
    pass
def extract():
    pass
def load():
    pass
# === Interface Utilisateur ===
st.title("🛠️ DataStack - Plateforme de Data Engineering")

# 1. Sélection de la source de données
st.sidebar.header("1️⃣ Charger les données")
source_type = st.sidebar.radio(
    "Choisissez la source de données",
    options=["Fichier local", "Base de données", "API"]
)

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
    db_connection = st.sidebar.text_input("Chaîne de connexion (SQLAlchemy)", "")
    db_query = st.sidebar.text_area("Requête SQL", "SELECT * FROM your_table")
    if st.sidebar.button("Charger depuis la base de données"):
        data = load_from_database(db_connection, db_query)
        if data is not None:
            st.success("Données chargées depuis la base de données.")
            st.write("Aperçu des données :")
            st.dataframe(data.head())

# 2. Nettoyage et exploration
if "data" in locals() and data is not None:
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

st.sidebar.info("Notre Application est evolutive - Nos modules seront ajouter au fur et à mesure pour enrichir cette plateforme.")

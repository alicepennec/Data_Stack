import streamlit as st
import pandas as pd
import requests
from sqlalchemy import create_engine
import sweetviz as sv
from streamlit_pandas_profiling import st_profile_report
from pandas_profiling import ProfileReport

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

def load_from_api(api_url, headers, params):
    """Charger des données depuis une API."""
    try:
        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
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

def generate_sweetviz_report(data):
    """Générer un rapport Sweetviz."""
    report = sv.analyze(data)
    report_file = "sweetviz_report.html"
    report.show_html(report_file, open_browser=False)
    return report_file

def generate_pandas_profiling_report(data):
    """Générer un rapport Pandas Profiling."""
    return ProfileReport(data, title="Pandas Profiling Report", explorative=True)

# === Interface Utilisateur ===
st.title("🛠️ DataStack - Plateforme de Data Engineering")

# 1. Sélection de la source de données
st.sidebar.header("1️⃣ Charger les données")
source_type = st.sidebar.radio(
    "Choisissez la source de données",
    options=["Fichier local", "API"]
)

data = None  # Initialisation de la variable pour éviter les erreurs

if source_type == "Fichier local":
    uploaded_file = st.sidebar.file_uploader("Téléversez votre fichier", type=["csv", "xlsx"])
    delimiter = st.sidebar.text_input("Délimiteur (par défaut : ',')", value=";")
    if uploaded_file is not None:
        data = load_local_file(uploaded_file, delimiter)
        if data is not None:
            st.success("Données chargées depuis le fichier local.")

elif source_type == "API":
    api_url = st.sidebar.text_input("URL de l'API", "https://api.example.com/data")
    headers_input = st.sidebar.text_area("En-têtes (format JSON)", '{"Authorization": "Bearer YOUR_TOKEN"}')
    params_input = st.sidebar.text_area("Paramètres (format JSON)", '{"key1": "value1", "key2": "value2"}')

    headers = {}
    params = {}
    try:
        if headers_input:
            headers = eval(headers_input)
        if params_input:
            params = eval(params_input)
    except Exception as e:
        st.error(f"Erreur dans le format des en-têtes ou des paramètres : {e}")

    if st.sidebar.button("Charger depuis l'API"):
        data = load_from_api(api_url, headers, params)
        if data is not None:
            st.success("Données chargées depuis l'API.")

# 2. Exploration des données (EDA)
if data is not None:
    st.sidebar.header("2️⃣ Exploration des données (EDA)")
    eda_option = st.sidebar.radio("Choisissez un outil EDA", ["Aperçu simple", "Sweetviz", "Pandas Profiling"])

    if eda_option == "Aperçu simple":
        st.subheader("🔍 Aperçu des données")
        st.write("**Aperçu des premières lignes :**")
        st.dataframe(data.head())
        st.write("**Résumé statistique :**")
        st.write(data.describe())

    elif eda_option == "Sweetviz":
        st.subheader("📊 Rapport Sweetviz")
        report_file = generate_sweetviz_report(data)
        with open(report_file, "rb") as f:
            st.download_button(
                label="Télécharger le rapport Sweetviz",
                data=f,
                file_name="sweetviz_report.html",
                mime="text/html",
            )

    elif eda_option == "Pandas Profiling":
        st.subheader("📋 Rapport Pandas Profiling")
        profile = generate_pandas_profiling_report(data)
        st_profile_report(profile)

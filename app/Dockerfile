FROM python:3.11

#Définir le répertoire de travail
WORKDIR /app

#Installer les dépendances à partir du fichier requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#Copier les fichiers nécessaires de l'application
COPY data_app_v1.py .

#Exposer le port utilisé par Streamlit
EXPOSE 8502

#Healthcheck pour vérifier si l'application est active
HEALTHCHECK --interval=30s --timeout=10s \
  CMD curl --fail http://localhost:8502/_stcore/health || exit 1

#Lancer l'application
CMD ["streamlit", "run", "data_app_v1.py", "--server.port=8502", "--server.address=0.0.0.0"]

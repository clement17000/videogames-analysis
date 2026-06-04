FROM apache/airflow:3.0.0

# Copie du fichier de dépendances généré par uv
COPY requirements.txt .

# Installation de toutes les dépendances (dlt, dbt, pandas, etc.)
RUN pip install --no-cache-dir -r requirements.txt
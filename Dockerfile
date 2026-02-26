# Base officielle pour les connecteurs Python Airbyte
FROM airbyte/python-connector-base:4.0.2

WORKDIR /airbyte/integration_code

COPY pyproject.toml poetry.lock* ./

# Optional mais recommandé pour la compatibilité
RUN pip install --upgrade pip setuptools wheel 

COPY . . 

RUN pip install --no-cache-dir . 

ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]

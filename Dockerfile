# Python
FROM python:3.13-slim

# Diretório de trabalho
WORKDIR /app

# Java 17 para Spark
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    ca-certificates \
    curl \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1


# Copia requerimentos e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia projeto
COPY . .

# Comando para executar
CMD ["python", "src/pipeline.py", "2025-09", "1", "1"]
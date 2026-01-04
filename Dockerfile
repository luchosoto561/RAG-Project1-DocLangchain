FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# deps del sistema mínimas (por si alguna lib compila wheels)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# primero requirements para cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copiamos SOLO la app runtime
COPY app ./app

# Cloud Run expone PORT
ENV PORT=8080

# arranque con uvicorn directo
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
"""
Embedder v1 (OpenAI-only)

API pública:
    - embed_texts(texts: List[str]) -> List[List[float]]

Detalles:
    - Usa OpenAI Embeddings (modelo por defecto: `text-embedding-3-small`).
    - Lee OPENAI_API_KEY del entorno (si tenés .env, lo carga si `python-dotenv`
      está disponible; si no, asumimos que exportaste la variable).
Exporta:
    MODEL_NAME: str
    DIMENSION: int
"""

from __future__ import annotations

import os
from typing import List

from openai import OpenAI

# Carga opcional de .env (no es requisito; si no está, sigue)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# -------- Config por entorno --------
MODEL_NAME = os.getenv("EMBEDDINGS_MODEL", "text-embedding-3-small")
BATCH_SIZE = int(os.getenv("EMBEDDINGS_BATCH_SIZE", "128"))

# Mapa de dimensiones conocidas
_DIMENSIONS = {
    "text-embedding-3-small": 1536,
    "text-embedding-3-large": 3072,
}
DIMENSION = _DIMENSIONS.get(MODEL_NAME, 0)  # si no conocemos, se completa tras la primera llamada


# -------- Cliente OpenAI (fail-fast) --------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise RuntimeError(
        "Falta OPENAI_API_KEY en el entorno (.env). "
        "Exportala o agrega al .env antes de correr."
    )

_client = OpenAI(api_key=OPENAI_API_KEY)


def embed_texts(texts: List[str]) -> List[List[float]]:
    """
    Recibe un *lote* de textos (cada texto = un chunk) y devuelve
    una lista de vectores (embeddings) en el mismo orden.

    No hace batching interno: asume que quien llama ya pasó "el lote".
    """
    if not texts:
        return []

    resp = _client.embeddings.create(model=MODEL_NAME, input=texts)
    embs = [d.embedding for d in resp.data]

    # Si no conocíamos la dimensión, la aprendemos en la primera respuesta.
    global DIMENSION
    if DIMENSION == 0 and embs:
        DIMENSION = len(embs[0])

    return embs


__all__ = ["embed_texts", "MODEL_NAME", "DIMENSION"]
"""
Embedder v1 (OpenAI-only)

API pública:
    - embed_texts(texts: List[str]) -> List[List[float]]

Detalles:
    - Usa OpenAI Embeddings (modelo por defecto: `text-embedding-3-small`).
    - Lee OPENAI_API_KEY del entorno (si tenés .env, lo carga si `python-dotenv`
      está disponible; si no, asumimos que exportaste la variable).
    - Batching simple, configurable por env:
        EMBEDDINGS_MODEL       (default: text-embedding-3-small)
        EMBEDDINGS_BATCH_SIZE  (default: 128)

Exporta:
    MODEL_NAME: str
    DIMENSION: int
"""

from __future__ import annotations

import logging
import os
import time
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


# -------- Cliente OpenAI --------


# La SDK lee OPENAI_API_KEY del entorno. Si falta, fallará al invocar.
_client = OpenAI()


def _embed_batch(texts: List[str]) -> List[List[float]]:
    """
    Llama a OpenAI para un lote de textos (sin dividir).
    Incluye reintentos simples ante errores transitorios (p. ej. rate limit). Y te
    retorna la lista de vectores que representa cada texto en la base de datos vectorial. 
    """
    clean = [(t if isinstance(t, str) else "") for t in texts]

    max_retries = 5
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = _client.embeddings.create(model=MODEL_NAME, input=clean)
            embs = [d.embedding for d in resp.data]  # orden 1:1 con `clean`
            # Si no teníamos dimensión, la inferimos ahora
            global DIMENSION
            if DIMENSION == 0 and embs:
                DIMENSION = len(embs[0])
            return embs
        except Exception as e:
            if attempt == max_retries:
                logging.error("OpenAI Embeddings falló tras %d reintentos: %s", attempt, e)
                raise
            logging.warning(
                "OpenAI Embeddings error (intento %d/%d): %s; reintento en %.1fs",
                attempt, max_retries, e, backoff
            )
            time.sleep(backoff)
            backoff *= 2.0  # backoff exponencial

    return []  # no debería ocurrir


def embed_texts(texts: List[str]) -> List[List[float]]:
    """
    Calcula embeddings para `texts` usando OpenAI.

    Args:
        texts: Lista de strings (cada uno será embebido).

    Returns:
        Lista de vectores (float) en el mismo orden que `texts`.
    """
    if not texts:
        return []

    out: List[List[float]] = []
    bs = max(1, int(BATCH_SIZE))
    for i in range(0, len(texts), bs):
        batch = texts[i : i + bs]
        out.extend(_embed_batch(batch))
    return out


__all__ = ["embed_texts", "MODEL_NAME", "DIMENSION"]
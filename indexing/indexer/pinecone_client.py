"""
Cliente Pinecone v1 (sencillo) para upserts de vectores + metadata.

Requisitos:
- Variables de entorno:
    - PINECONE_API_KEY   (obligatoria)
    - PINECONE_INDEX     (obligatoria)
- El índice debe existir previamente y tener la dimensión correcta para tus embeddings.

Interfaz pública:
- upsert(namespace: str, items: List[Dict[str, Any]]) -> int
    Inserta/actualiza los items en el índice dado el namespace. Devuelve cuántos vectors
    se enviaron (longitud de `items`). Maneja reintentos básicos ante errores transitorios.

Notas v1:
- No crea el índice. Si necesitás creación automática, añadilo más adelante.
- Usa un backoff simple para upserts.
"""

from __future__ import annotations

import os
import time
import logging
from typing import Any, Dict, List, Optional

# Dependencia oficial del SDK nuevo de Pinecone:
#   pip install pinecone
try:
    from pinecone import Pinecone  # SDK moderno (>=3.x)
except Exception as e:  # pragma: no cover
    raise RuntimeError(
        "No se pudo importar 'pinecone'. Instalá el SDK con: pip install pinecone"
    ) from e


# -------------------------------
# Config y singletons de cliente
# -------------------------------

_PC: Optional[Pinecone] = None
_INDEX = None
_INDEX_NAME: Optional[str] = None


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Falta variable de entorno requerida: {name}")
    return val


def _init_client() -> None:
    """Inicializa el cliente global y el objeto Index a partir de env vars."""
    global _PC, _INDEX, _INDEX_NAME
    if _PC is not None and _INDEX is not None:
        return

    api_key = _require_env("PINECONE_API_KEY")
    _INDEX_NAME = _require_env("PINECONE_INDEX")

    logging.info("Conectando a Pinecone (index=%s)...", _INDEX_NAME)
    _PC = Pinecone(api_key=api_key)
    _INDEX = _PC.Index(_INDEX_NAME)
    logging.info("Pinecone listo.")


def _get_index():
    """Devuelve el objeto Index (inicializando si hace falta)."""
    if _INDEX is None:
        _init_client()
    return _INDEX


# -------------------------------
# Upsert con backoff simple
# -------------------------------

def upsert(
    *,
    items: List[Dict[str, Any]],
    namespace: str, 
    max_retries: int = 3,
    initial_backoff_s: float = 1.0,
) -> int:
    """
    Sube/actualiza vectores en Pinecone.

    Args:
        namespace: Segmento lógico dentro del índice (p.ej. "dev", "prod").
        items: Lista de dicts con las claves: "id" (str), "vector" (List[float]), "metadata" (dict).
               Ejemplo de cada item:
                 {
                   "id": "abc123",
                   "vector": [0.1, 0.2, ...],
                   "metadata": {"url_citable": "...", "title": "...", ...}
                 }
        max_retries: Reintentos ante fallos transitorios.
        initial_backoff_s: Pausa inicial entre reintentos; duplica en cada intento.

    Returns:
        int: Cantidad de vectores enviados (len(items)).

    Errores:
        - Levanta ValueError si items está vacío o faltan campos.
        - Propaga la última excepción si todos los reintentos fallan.
    """
    if not items:
        raise ValueError("upsert: 'items' no puede estar vacío.")

    # Validación mínima de estructura (v1)
    for it in items:
        if "id" not in it or "vector" not in it:
            raise ValueError("Cada item debe tener 'id' y 'vector'.")
        if not isinstance(it["id"], str):
            raise ValueError("El 'id' debe ser str.")
        if not isinstance(it["vector"], list):
            raise ValueError("'vector' debe ser una lista de floats.")
        # metadata es opcional en Pinecone, pero nosotros solemos enviarla
        if "metadata" in it and not isinstance(it["metadata"], dict):
            raise ValueError("'metadata' (si está) debe ser dict JSON-serializable.")

    index = _get_index()

    # Backoff simple (exponencial)
    delay = max(0.1, float(initial_backoff_s))
    attempt = 0
    last_exc: Optional[BaseException] = None

    while attempt <= max_retries:
        try:
            # El SDK acepta directamente la lista de dicts con id/vector/metadata
            # Ver: index.upsert(vectors=..., namespace="...")
            index.upsert(vectors=items, namespace=namespace)
            logging.info("Upsert OK: %d vectores (namespace=%s)", len(items), namespace)
            return len(items)
        except Exception as e:
            last_exc = e
            attempt += 1
            if attempt > max_retries:
                logging.error("Upsert falló definitivamente tras %d reintentos: %s", max_retries, repr(e))
                break
            logging.warning(
                "Upsert falló (intento %d/%d): %s. Reintentando en %.1fs...",
                attempt, max_retries, repr(e), delay
            )
            time.sleep(delay)
            delay *= 2  # exponencial

    # Si llegamos acá, fallaron todos los intentos
    assert last_exc is not None
    raise last_exc


__all__ = ["upsert"]

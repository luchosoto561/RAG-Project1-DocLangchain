# indexing/ingest_index.py
"""
Driver de ingestión v1.

Flujo:
- Descubre archivos JSONL en `data/chunks/` (por defecto) o los que se le pasen.
- Lee línea por línea (cada línea = un chunk dict JSON-serializable).
- Lotea los textos, llama a `embedder.embed_batch(chunks)` -> List[List[float]].
- Empaqueta (id, vector, metadata) y llama a `pinecone_client.upsert_batch(items)`.
- Opcionalmente borra el namespace antes de empezar (--reset-namespace).

Dependencias esperadas (no implementadas acá):
- indexing/embedder.py con: `embed_batch(chunks: List[dict]) -> List[List[float]]`
- indexing/pinecone_client.py con:
    - `reset_namespace(namespace: str) -> None`
    - `upsert_batch(items: List[dict], *, namespace: str) -> None`

Uso:
    python -m indexing.ingest_index \
        --chunks-dir data/chunks \
        --namespace langchain-docs \
        --batch-size 128 \
        --reset-namespace

Notas:
- Solo usa stdlib.
- Maneja errores por línea con log y continúa.
- El metadata enviado al vector store es un subconjunto estable de los campos del chunk.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Any, Dict, Generator, Iterable, List, Optional, Tuple
import embedder as EMB
import pinecone_client as PC

# -----------------------------------------------------------------------------#
# Configuración de logging
# -----------------------------------------------------------------------------#

LOG = logging.getLogger("ingest")


# -----------------------------------------------------------------------------#
# Utilidades de archivos / batches
# -----------------------------------------------------------------------------#

def iter_jsonl_files(chunks_dir: Path) -> List[Path]:
    """
    Devuelve una lista ORDENADA de todos los archivos *.jsonl bajo `chunks_dir` (recursivo).

    Args:
        chunks_dir: Carpeta base donde el chunker escribe los JSONL (p.ej. data/chunks).

    Returns:
        List[Path]: Rutas a archivos .jsonl encontradas (puede ser lista vacía).

    Raises:
        FileNotFoundError: Si `chunks_dir` no existe.
        NotADirectoryError: Si `chunks_dir` existe pero no es carpeta.
    """
    if not chunks_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta de chunks: {chunks_dir}")
    if not chunks_dir.is_dir():
        raise NotADirectoryError(f"No es una carpeta: {chunks_dir}")

    files = sorted(chunks_dir.rglob("*.jsonl"))
    if not files:
        logging.warning("No se encontraron archivos .jsonl en %s", chunks_dir)

    logging.info("Archivos .jsonl a ingerir: %d", len(files))
    return files


def read_jsonl(path: Path) -> Generator[Dict[str, Any], None, None]:
    """
    Lee un archivo .jsonl y devuelve dicts (los chunks) imprime info. y salta líneas inválidas (si tiene una linea
    que no es json valido, no se cae el programa sino que loguea la linea problematica y sigue con la siguiente).
    """
    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
                else:
                    LOG.warning("[%s:%d] línea no es un objeto JSON (se ignora)", path.name, ln)
            except Exception as e:
                LOG.warning("[%s:%d] JSON inválido (%s) (se ignora)", path.name, ln, e)


def batch_iter(it: Iterable[Any], batch_size: int) -> Generator[List[Any], None, None]:
    """
    Agrupa un iterable (en nuestro caso read_jsonl, es decir, todos los chunks de un .jsonl) en listas (lotes -> se usan para pasarle a 
    Pinecone varios chunks de una) de hasta "batch_size" elementos.
    """
    buf: List[Any] = []
    for x in it:
        buf.append(x)
        if len(buf) >= batch_size:
            yield buf
            buf = []
    if buf:
        yield buf


# -----------------------------------------------------------------------------#
# Transformaciones
# -----------------------------------------------------------------------------#

# metadata por chunk
META_FIELDS = (
    "url_final",
    "url_citable",
    "title",
    "section_level",
    "section_heading",
    "section_anchor",
    "fetched_at",
    "has_code",
)


def to_metadata(chunk: Dict[str, Any]) -> Dict[str, Any]:
    """
    selecciona los metadatos para el chunk, teniendo en cuenta de los que estan en META_FIELDS, los que tiene el chunk.
    """
    md = {k: chunk.get(k) for k in META_FIELDS if k in chunk}
    # Tip: acá podrías normalizar anchors/títulos en v2 si quisieras.
    return md


def prepare_upsert_payload(
    chunks: List[Dict[str, Any]],
    vectors: List[List[float]],
) -> List[Dict[str, Any]]:
    """
    Recibe la lista de chunks y la lista de vectores que representan a cada chunk. El chunk en la pos 0 corresponde al vector en la pos 0 y asi.
    devuelve una lista de diccionarios, los cuales ya estan listos para upsert en la base de datos vectorial. 
    """
    if len(chunks) != len(vectors):
        raise ValueError(f"Desalineo chunks/vectors: {len(chunks)} != {len(vectors)}")

    items: List[Dict[str, Any]] = []
    for ch, vec in zip(chunks, vectors):
        cid = ch.get("id")
        if not cid:
            raise ValueError("Chunk sin 'id'; no se puede upsertear.")
        items.append(
            {
                "id": str(cid),
                "vector": vec,
                "metadata": to_metadata(ch),
            }
        )
    return items


# -----------------------------------------------------------------------------#
# Orquestación principal
# -----------------------------------------------------------------------------#

def ingest_file(
    path: Path,
    *,
    batch_size: int,
    namespace: str,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """
    Esta funcion se encarga de tomar un archivo .jsonl de chunks y meterlos en la base de datos vectorial en tandas. 
    
    Parametros:
    - path: Path -> ruta al archivo jsonl que generlo el chunker
    - batch_size: int -> cuantos chunks procesa por tandas
    - namespace: str -> "carpeta" logica dentro del indice de Pinecone donde vas a guardar estos vectores (te permite separar, por ej, langchain docs, otra fuente, etc sin mezclar resultados)
    - dry_run: bool -> si esta en True hace todo menos el upsert, util para probar el pipeline. 
    
    Devuelve:
    - (ingresados, fallidos)
    """

    n_chunks = 0
    n_batches = 0

    LOG.info("Procesando: %s", path.name)

    # Stream de chunks del archivo
    for batch_chunks in batch_iter(read_jsonl(path), batch_size=batch_size):
        # Para v1, el texto del embedding es el campo "text" tal cual.
        texts_missing = [c for c in batch_chunks if not isinstance(c.get("text"), str)]
        if texts_missing:
            LOG.warning("Saltando %d chunks sin 'text' válido en %s", len(texts_missing), path.name)
            batch_chunks = [c for c in batch_chunks if c not in texts_missing]
            if not batch_chunks:
                continue

        if dry_run:
            # En dry-run no llamamos a modelos ni a Pinecone
            LOG.debug("[dry-run] batch de %d chunks (namespace=%s)", len(batch_chunks), namespace)
            n_chunks += len(batch_chunks)
            n_batches += 1
            continue

        # 1) Embeddings
        texts = [c.get("text", "") for c in batch_chunks]    
        vectors = EMB.embed_texts(texts)
    
        # 2) Upsert
        items = prepare_upsert_payload(batch_chunks, vectors)
        PC.upsert(items, namespace=namespace)

        n_chunks += len(batch_chunks)
        n_batches += 1

    LOG.info("OK %s — %d chunks en %d batches", path.name, n_chunks, n_batches)
    return n_chunks, n_batches



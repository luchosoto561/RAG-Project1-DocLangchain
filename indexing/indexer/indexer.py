"""
Indexación v1 (script simple, sin funciones ni CLI).

- Recorre todos los .jsonl en data/chunks
- Para cada archivo, hace embeddings + upsert en el namespace indicado
- Loguea progreso y un resumen final

"""

from __future__ import annotations

from pathlib import Path
import logging

# Traemos utilidades desde el módulo de ingesta “real”
from ingest_index import iter_jsonl_files, ingest_file

# -----------------------------
# Parámetros “fijos” v1
# Cambiá estos valores según tu entorno
# -----------------------------
CHUNKS_DIR = Path("data/chunks")  # carpeta donde el chunker escribe los .jsonl
NAMESPACE = "dev"                 # p.ej. "dev" o "prod"
BATCH_SIZE = 64                   # tamaño de lote para embeddings + upsert

# -----------------------------
# Logging básico
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logging.info(
    "Iniciando indexación: dir=%s namespace=%s batch=%d",
    CHUNKS_DIR, NAMESPACE, BATCH_SIZE,
)

# -----------------------------
# Recorrido de archivos y ejecución
# -----------------------------
files = iter_jsonl_files(CHUNKS_DIR)
total_ok = 0
total_err = 0

for i, path in enumerate(files, start=1):
    logging.info("(%d/%d) Ingeriendo archivo: %s", i, len(files), path)
    ok, err = ingest_file(
        path,
        batch_size=BATCH_SIZE,
        namespace=NAMESPACE,
    )
    total_ok += ok
    total_err += err

summary = {"chunks procesados " "ok": total_ok, "errors": total_err}
logging.info("Indexación terminada: %s", summary)

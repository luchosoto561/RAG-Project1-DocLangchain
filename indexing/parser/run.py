# parser/run.py
# v1 — Orquestador mínimo del parser:
# - Lee index.json
# - Filtra registros válidos (status_code == 200 y archivo HTML existente).
# - Carga el HTML crudo, llama a core.parse_document(...)
# - Persiste el JSON resultante en data/parsed_pages/<host>/<YYYYMMDD>/<sha1(url_final)>.json

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Union

# Importa SOLO lo necesario del core del parser (lógica pura, sin I/O).
# La implementaremos luego en parser/core.py con la firma indicada.
from core import parse_document  # type: ignore

# recibe en nuestro caso la url_final
def _sha1(text: str) -> str:
    """Hash SHA1 hex de un string (para nombres de archivo deterministas)."""
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def _date_stamp_iso(date_str: str) -> str:
    """
    Convierte un timestamp ISO (fetched_at) a AAAAMMDD para carpetas.
    Acepta ISO con o sin microsegundos. Si falla, usa 'unknown'.
    """
    try:
        # Intentar con microsegundos
        dt = datetime.fromisoformat(date_str)
    except ValueError:
        return "unknown"
    return dt.strftime("%Y%m%d")

# recorre el index.json y va devolviendo un dict en cada iteracion de la funcion, esto lo entiendo porque la funcion no usa return sino que usa yield, entonces se crea un generador que se escribe como Iterator[Dict] en este caso de Dict
def _load_index(path: Union[str, Path]) -> Iterator[Dict]:
    """
    Carga un índice que puede estar en:
    - JSON (array grande con registros)
    - JSONL (una línea por registro)
    Devuelve un iterador de dicts (registros).
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Índice no encontrado: {p}")

    # Heurística simple por extensión y/o primer carácter
    if p.suffix.lower() == ".jsonl":
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # recorda que el yield hace que cuando se llame a la funcion en un for te retorne y se pause la funcion, para que luego en el prox llamado se sigua de la linea siguiente con las variables locales y el estado intactos
                yield json.loads(line)
        return

    # Si es .json (array) o no tiene extensión, intentamos cargar todo
    with p.open("r", encoding="utf-8") as f:
        content = f.read().lstrip()
        if content.startswith("["):
            # JSON array
            for rec in json.loads(content):
                yield rec
        else:
            # Permitir JSONL aunque la extensión no sea .jsonl
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)

 # devuelve igual que la anterior un generador
def _iter_valid_records(index_path: Union[str, Path]) -> Iterator[Dict]:
    """
    Filtra registros válidos para parsear:
    - status_code == 200
    - html_crudo_path existente
    - url_final presente (requisito para citas)
    """
    for rec in _load_index(index_path):
        if rec.get("status_code") != 200:
            continue
        html_path_raw = rec.get("html_crudo_path")
        if not html_path_raw:
            continue

        # Normaliza separadores (soporta rutas con "\" guardadas en Windows)
        html_path = Path(str(html_path_raw).replace("\\", "/"))
        if not html_path.exists():
            continue

        url_final = rec.get("url_final") or rec.get("url")  # fallback defensivo
        if not url_final:
            continue

        fetched_at = rec.get("fetched_at") or ""
        host = rec.get("host") or "unknown-host"

        yield {
            "html_path": html_path,
            "url_final": str(url_final),
            "fetched_at": str(fetched_at),
            "host": str(host),
        }


def _output_path(base_dir: Union[str, Path],# path donde vas a guardar el json
                 host: str, # host del documento por ejemplo python.langchain.com
                 fetched_at: str, # timestamp del ISO de cuando se descargo el html
                 url_final: str) -> Path: 
    """
    Construye la ruta de salida para el JSON parseado:
    data/parsed_pages/<host>/<YYYYMMDD>/<sha1(url_final)>.json
    """
    date_stamp = _date_stamp_iso(fetched_at)
    fname = f"{_sha1(url_final)}.json"
    return Path(base_dir) / host / date_stamp / fname


def run_parser(
    index_path: Union[str, Path], # path a index.json
    out_base_dir: Union[str, Path] = "data/parsed_pages", # carpeta base donde se van a escribir los json parseados
    limit: int | None = None, # tope de cuantos documentos procesar en esta corrida
) -> Dict[str, int]:
    """
    Orquesta el parseo de múltiples HTML según el índice.
    Devuelve contadores simples.
    """
    # carpeta base donde van todos los json parseados
    out_dir = Path(out_base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    processed = 0
    written = 0
    skipped = 0
    errors = 0

    for rec in _iter_valid_records(index_path):
        if limit is not None and processed >= limit:
            break

        processed += 1
        html_path: Path = rec["html_path"]
        url_final: str = rec["url_final"]
        fetched_at: str = rec["fetched_at"]
        host: str = rec["host"]

        try:
            html_text = html_path.read_text(encoding="utf-8", errors="replace")

            # Llamada a la lógica pura del parser (core).
            doc = parse_document(html_text=html_text, url_final=url_final, fetched_at=fetched_at)
            if not doc or not isinstance(doc, dict):
                skipped += 1
                continue
            # carpeta donde va el json asociado al html de esta iteracion
            out_path = _output_path(out_base_dir, host, fetched_at, url_final)
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # Persistimos tal cual devuelve el core (sin mutarlo aquí).
            out_path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
            written += 1

        except Exception:
            # No propagamos para que el batch continúe; contamos y seguimos.
            errors += 1

    return {
        "processed": processed,
        "written": written,
        "skipped": skipped,
        "errors": errors,
    }


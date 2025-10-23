"""
Recorre todos los .json parseados bajo:
  data/parsed_pages/langchain-ai.github.io/**/*.json
  data/parsed_pages/python.langchain.com/**/*.json
Para cada archivo: carga, hace make_chunks y emite JSONL por stdout.

"""
from pathlib import Path
import sys, json
from core import make_chunks


base = Path("data/parsed_pages")
hosts = ("langchain-ai.github.io", "python.langchain.com")


def _jsonl_path_for_host(host: str) -> Path:
    out_dir = Path("data/chunks")
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{host}.jsonl"   # nombre fijo -> se pisa en cada corrida



for host in hosts:
    host_dir = base / host
    if not host_dir.exists():
        continue

    out_path = _jsonl_path_for_host(host)

    # "w" = sobreescribe el archivo si ya existe
    with out_path.open("w", encoding="utf-8") as out_f:
        for path in host_dir.rglob("*.json"):
            try:
                with path.open("r", encoding="utf-8") as f:
                    parsed_page = json.load(f)
                chunks = make_chunks(parsed_page)

                for ch in chunks:
                    line = json.dumps(ch, ensure_ascii=False)
                    out_f.write(line + "\n")      # guarda en JSONL (se pisa en cada run)
                    sys.stdout.write(line + "\n")  # opcional: streaming por stdout
            except Exception as e:
                print(f"[chunker] error en {path}: {e}", file=sys.stderr)

    print(f"[chunker] escrito: {out_path}", file=sys.stderr)
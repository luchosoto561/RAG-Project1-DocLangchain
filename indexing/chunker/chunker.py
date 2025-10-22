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

for host in hosts:
    host_dir = base / host
    if not host_dir.exists():
        continue
    for path in host_dir.rglob("*.json"):
        try:
            with path.open("r", encoding="utf-8") as f:
                parsed_page = json.load(f)
            chunks = make_chunks(parsed_page)
            for ch in chunks:
                sys.stdout.write(json.dumps(ch, ensure_ascii=False) + "\n")
        except Exception as e:
            print(f"[chunker] error en {path}: {e}", file=sys.stderr)
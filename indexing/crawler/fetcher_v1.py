# baja las urls validas del manifest (semillas), guarda el html crudo y deja un registro de cada descarga. No sigue enlaces (profundidad cero)

from __future__ import annotations
from pathlib import Path
import json, hashlib, datetime as dt
from urllib.parse import urlsplit, urlunsplit
import urllib.robotparser as robotparser

import httpx  

USER_AGENT = "RAG-Langchain-Fetcher/0.1 (lucianofranciscosoto@gmail.com)"
TIMEOUT = 15.0

# Ajustá esta ruta si tu manifest quedó en otro lado.
MANIFEST_PATH = Path("indexing/crawler/seeds_manifest.json")

RAW_DIR = Path("data/raw_pages")
INDEX_PATH = Path("data/raw_pages/index.jsonl")

# genera un id estable a partir de un texto (la url) usando hash SHA-1, es decir, obtenemos un nombre corto, seguro y repetible que representa a una url.
def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

# devuelve la fecha de hoy en formato YYYYMMDD para armar carpetas por dia
def today_stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d")

# lee el manifest y devuelve la lista de URLS validas, sin repetidas
def load_valid_seed_urls(manifest_path: Path) -> list[str]:
    """Lee el manifest y devuelve la lista de URLs válidas (deduplicadas)."""
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    urls: list[str] = []
    for g in data["groups"]:
        urls.extend(g.get("valid_urls", []))
    # dedup simple conservando orden aproximado
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

# consulta el robots.txt del sitio y dice si tenemos permisos para bajar esa url
def robots_allows(url: str) -> bool:
    """Consulta robots.txt del host y verifica si se permite fetch de 'url'."""
    parts = urlsplit(url)
    robots_url = urlunsplit((parts.scheme, parts.netloc, "/robots.txt", "", ""))
    
    # crea un interprete de robots.txt de la libreria estandar, este objeto sabe leer reglas, y luego responder si tu bot puede o no  visitar una URL
    rp = robotparser.RobotFileParser()
    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, follow_redirects=True) as client:
            r = client.get(robots_url)
            if r.status_code == 200:
                rp.parse(r.text.splitlines())
            else:
                # Si no hay robots.txt, por defecto permitimos.
                rp.parse(["User-agent: *", "Allow: /"])
    except Exception:
        rp.parse(["User-agent: *", "Allow: /"])
    return rp.can_fetch(USER_AGENT, url)

# descarga la pagina si es html y 200 ok, la guarda en data/raw_pages/... y arma un registro con el resultado
def fetch_and_save(url: str) -> dict:
    """Descarga una URL y guarda el HTML crudo si es text/html.
    Devuelve un registro con metadatos (para index.jsonl)."""
    host = urlsplit(url).netloc
    fetched_at = dt.datetime.now().isoformat()
    rec = {
        "url": url,
        "host": host,
        "status_code": None,
        "fetched_at": fetched_at,
        "html_crudo_path": None,
        "error": None,
    }

    if not robots_allows(url):
        rec["error"] = "robots_disallow"
        return rec

    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, follow_redirects=True) as client:
            resp = client.get(url)
        rec["status_code"] = resp.status_code
        if resp.status_code != 200:
            rec["error"] = f"http_{resp.status_code}"
            return rec

        ctype = resp.headers.get("Content-Type", "").lower()
        if "text/html" not in ctype:
            rec["error"] = "non_html"
            return rec

        # page_id estable a partir de la URL normalizada del manifest
        pid = sha1(url)
        out_dir = RAW_DIR / host / today_stamp()
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{pid}.html"
        out_path.write_text(resp.text, encoding="utf-8")
        rec["html_crudo_path"] = str(out_path)
        return rec

    except httpx.RequestError as e:
        rec["error"] = f"network:{type(e).__name__}"
        return rec

# agrega ese registro como una linea en data/raws_pages/index.json1
def append_index(rec: dict) -> None:
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    with INDEX_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


# orquesta todo, carga las urls, las descarga una por una y al final muestra un resumen
if __name__ == "__main__":
    if not MANIFEST_PATH.exists():
        raise SystemExit(f"No existe {MANIFEST_PATH}. Corré primero seeds_loader.py")

    urls = load_valid_seed_urls(MANIFEST_PATH)
    print(f"Descargando {len(urls)} seeds…")
    ok = 0
    for u in urls:
        rec = fetch_and_save(u)
        append_index(rec)
        ok += int(rec["html_crudo_path"] is not None)
    print(f"✔ Listo. OK: {ok} | Ver data/raw_pages/ y {INDEX_PATH}")

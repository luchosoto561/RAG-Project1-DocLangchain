"""
    baja las urls validas del seeds_manifest.json (semillas), guarda el html crudo y deja un registro de cada descarga. No sigue enlaces (profundidad cero)

"""
# estandar
from __future__ import annotations
from pathlib import Path
# hashlib: funciones hash (entrada: bytes; salida: digest()->bytes, hexdigest()->str hex)
import json, hashlib, datetime as dt
from typing import TypedDict, Optional
import re
from urllib.parse import urlsplit, urlunsplit, urlparse, urljoin
import urllib.robotparser as robotparser

# terceros
import httpx  

class Dict_por_url(TypedDict):
        url: str
        url_final: str
        host: str
        status_code: Optional[int]
        fetched_at: str
        html_crudo_path: Optional[str]
        error: Optional[str]
        rl_wait_s_total: int
        retries: int
        last_backoff_s: int

# cadena de texto que se manda en la cabecera a una web por ej la doc oficial de langchain, sirve para identificar quien hace la peticion
USER_AGENT = "RAG-Langchain-Fetcher/0.1 (lucianofranciscosoto@gmail.com)"
TIMEOUT = 15.0

# Ajustá esta ruta si tu manifest quedó en otro lado.
MANIFEST_PATH = Path("indexing/crawler/seeds_manifest.json")

RAW_DIR = Path("data/raw_pages")
INDEX_PATH = Path("data/raw_pages/index.json")

def sha1(s: str) -> str:
    """
    crea un objeto hash SHA-1 inicializado con esos bytes y .hexdigest() finaliza el computo y devuelve el resumen en hexadecimal 
    RESULTADO : La funcion devuelve el SHA-1 en hex de la cadena s (codificada en UTF-8), es determinista, es decir, misma s, mismo resultado, asi es como se 
    convierte cualquier string en una huella unica de tamano fijo
    """
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def today_stamp() -> str:
    """
    devuelve la fecha de hoy en formato YYYYMMDD para armar carpetas por dia
    """
    return dt.datetime.now().strftime("%Y%m%d")

def load_valid_seed_urls(manifest_path: Path) -> list[str]:
    """
    lee el manifest y devuelve la lista de URLS validas, sin repetidas
    """
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    urls: list[str] = []
    # agrega las url validas en urls
    for g in data["groups"]:
        # agrega las urls dentro de cada lista perteneciente a groups, a extend se le pasa un iterable y el agrega todos los elementos, el [] se pone porque si no existe la clave valid_urls se considera vacio
        urls.extend(g.get("valid_urls", []))
    # dedup simple conservando orden aproximado
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def robots_allows(url: str) -> bool:
    """
    consulta el robots.txt del sitio y dice si tenemos permisos para acceder a la url pasada como parametro
    rp.parse(lineas de texto del bot.txt) -> limpia las lineas quitando espacios, etc, agrupa por user-agent las
    reglas
    """
    parts = urlsplit(url)
    robots_url = urlunsplit((parts.scheme, parts.netloc, "/robots.txt", "", ""))
    
    # rp sera quien responda si una url esta permitida para un user-agent
    rp = robotparser.RobotFileParser()
    try:
        # follow_redirects=True indica que siga las redirecciones que tenga que seguir hasta llegar al robots.txt final, sino si tuviera que seguir una redireccion, no se seguiria
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


_CANONICAL_RE = re.compile(
    r'<link[^>]+rel=["\']canonical["\'][^>]*href=["\']([^"\']+)["\']',
    re.IGNORECASE
)

def _pick_url_final(requested_url: str, response) -> str:
    """
    Devuelve la URL final para citar:
    1) URL efectiva (response.url) tras redirecciones.
    2) Si hay <link rel="canonical"> confiable (mismo host, http/https), la prefiere.
    """
    effective_url = str(getattr(response, "url", requested_url) or requested_url)

    try:
        html = getattr(response, "text", "") or ""
        m = _CANONICAL_RE.search(html)
        if m:
            href = m.group(1).strip()
            cand = urljoin(effective_url, href)  # resuelve canonicals relativos
            eff = urlparse(effective_url)
            can = urlparse(cand)
            if can.scheme in ("http", "https") and (can.netloc or "").lower() == (eff.netloc or "").lower():
                return cand
    except Exception:
        pass

    return effective_url

# descarga la pagina si es html y 200 ok, la guarda en data/raw_pages/... y retorna un registro con el resultado
def fetch_and_save(url: str) -> dict:
    """Descarga una URL y guarda el HTML crudo si es text/html.
    Devuelve un registro con metadatos (para index.jsonl)."""
    host = urlsplit(url).netloc
    fetched_at = dt.datetime.now().isoformat()
    rec = {
        "url": url,
        "url_final": None,
        "host": host,
        "status_code": None,
        "fetched_at": fetched_at,
        "html_crudo_path": None,
        "error": None,
        "rl_wait_s_total": 0,
        "retries": 0,
        "last_backoff_s": 0 
    }

    if not robots_allows(url):
        rec["error"] = "robots_disallow"
        return rec

    try:
        with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, follow_redirects=True) as client:
            resp = client.get(url)
        rec["url_final"] = _pick_url_final(url, resp)
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
        rec.setdefault("url_final", url)
        return rec

def append_index(recs: list[Dict_por_url]) -> None:
    """agrega ese registro como una linea en data/raws_pages/index.json1"""
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(recs, ensure_ascii=False, indent=2), encoding="utf-8")


def fetcher_v1() -> None:
    """ejecuta todo el fetcher -> descarga las urls sin repetidas y crea json con info de las descargasindex.jsonl
    """
    urls_a_descargar = load_valid_seed_urls(Path("indexing/crawler/seeds_manifest.json"))

    # La “lista grande” de registros
    lista_diccionarios_por_url: list[Dict_por_url] = []
    
    for url in urls_a_descargar:
        # se descarga la url y se crea descripcion de la url
        lista_diccionarios_por_url.append(fetch_and_save(url))
        
    append_index(lista_diccionarios_por_url)



# dejamos todas las urls con una forma unica y limpia, validamos dominios, verificando que cada url apunte solo a los dominios oficiales, generamos un resumen ej formato JSON,
# que especifica que archivos .txt lei, cuantas urls validas, invalidas por grupos, la lista de urls que efectivamente se van a  usar como punto de partida

from __future__ import annotations
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, urljoin, quote, unquote
import json
import datetime as dt

# ----- Config mínima (podés cambiar SEEDS_DIR sin tocar lógica)
SEEDS_DIR = Path("indexing/crawler")  
SEED_FILES = {
    "langchain_python": "langchain_python.txt",
    "langgraph":        "langgraph.txt",
    "langsmith":        "langsmith.txt",
}
ALLOWED_HOSTS = {
    "python.langchain.com",
    "langchain-ai.github.io",
    "docs.langchain.com",
}

# son query params que vamos a sacar de las url, asi no tenemos urls que apuntan a lo mismo pero con distintos parametros que indican cuestiones de marketing, etc.  
TRACKING_PARAMS = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","fbclid","gclid","ref"}


#limpia la query de la url quitando parametros de tracking que no cambian el contenido
def strip_tracking(url: str) -> str:
    parts = list(urlsplit(url))
    if parts[3]:  # query
        kept = []
        for kv in parts[3].split("&"):
            if not kv: 
                continue
            k = kv.split("=", 1)[0]
            if k in TRACKING_PARAMS:
                continue
            kept.append(kv)
        parts[3] = "&".join(kept)
    return urlunsplit(tuple(parts))

# deja a la url en una forma unica para descargar
def normalize_url(raw: str, base: str | None = None) -> str:
    """https, host minúsculas, sin fragmento, sin tracking, trailing slash para paths de carpeta."""
    if base:
        raw = urljoin(base, raw)
    parts = list(urlsplit(raw))
    scheme, netloc, path, query, fragment = parts
    scheme = "https" if scheme in ("http","https") else scheme
    netloc = netloc.lower()
    fragment = ""  # no usamos #fragment para descargar
    # path limpio
    path = quote(unquote(path))
    if "." not in path.rsplit("/", 1)[-1] and not path.endswith("/"):
        path += "/"
    url = urlunsplit((scheme, netloc, path, query, fragment))
    return strip_tracking(url)

# lee un archivo .txt de indexing/crawler, ignora lineas vacias o que empiezan con # y normaliza cada url con normalize_url
def load_seed_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    urls = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(normalize_url(line))
    return urls

# separa las url en validas e invalidas segun el dominio. Compara el host de cada url con el set ALLOWED_HOSTS 
def validate_hosts(urls: list[str]) -> tuple[list[str], list[str]]:
    ok, bad = [], []
    for u in urls:
        host = urlsplit(u).netloc
        (ok if host in ALLOWED_HOSTS else bad).append(u)
    return ok, bad

# arama un diccionario con las urls ya lindas para langchain, langgraph y langsmith
def load_all_seeds(seeds_dir: Path = SEEDS_DIR) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for group, fname in SEED_FILES.items():
        urls = load_seed_file(seeds_dir / fname)
        out[group] = urls
    return out

# recibe el dict de la funcion de arriba, y para cada grupo valida hosts con validate_hosts, calcula contenedores (count_total, count_valid, count_invalid), guarda listas de valid_urls y invalid_urls. Luego escribe un json con esa info y metadatos minimos
def write_manifest(seeds: dict[str, list[str]], out_path: Path = Path("indexing/crawler/seeds_manifest.json")) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": dt.datetime.now().isoformat(),
        "seeds_dir": str(SEEDS_DIR),
        "groups": [],
    }
    for group, urls in seeds.items():
        ok, bad = validate_hosts(urls)
        payload["groups"].append({
            "name": group,
            "count_total": len(urls),
            "count_valid": len(ok),
            "count_invalid": len(bad),
            "valid_urls": ok,
            "invalid_urls": bad,
        })
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

#permite correr el archivo no solo importarlo
if __name__ == "__main__":
    seeds = load_all_seeds()
    write_manifest(seeds)
    print("✔ seeds_manifest.json generado en data/manifests/")

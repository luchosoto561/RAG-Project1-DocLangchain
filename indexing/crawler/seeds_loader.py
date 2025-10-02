""" 
con mis palabras -> dejamos todas las urls con una forma unica y limpia, validamos dominios, verificando que cada url apunte solo a los dominios oficiales, generamos un resumen ej formato JSON,
que especifica que archivos .txt lei, cuantas urls validas, invalidas por grupos, la lista de urls que efectivamente se van a  usar como punto de partida

segun chat gpt -> Este script lee tus URLs semilla desde los .txt en indexing/crawler/, las normaliza (fuerza https, host en minúsculas, quita #fragment, limpia params de tracking tipo utm_*,
agrega / si es “carpeta”), valida que pertenezcan a los dominios oficiales y genera un manifest JSON (seeds_manifest.json) con un resumen por grupo: cuántas había en total, 
cuántas son válidas/ inválidas y las listas de cada una. Ese manifest es la “lista limpia” que después usa el fetcher para descargar páginas.

"""
from __future__ import annotations
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit, urljoin, quote, unquote
import json
import datetime as dt
from typing import Dict, List


SEEDS_DIR = Path("indexing/crawler")  

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
            # separa el kv que es el parametro=valor, como maximo hace una separacion, es decir en dos pedazos y accede a la posicion 0, el nombre del parametro
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
        # si raw es una url absoluta te devuelve raw, si empieza con / reeplaza el path de base por raw y te devuelve la url absoluta, si raw empieza sin /, te agrega al directorio donde apunta la url a raw. Basicamente vas a tener una url absoluta
        raw = urljoin(base, raw)
    parts = list(urlsplit(raw))
    scheme, netloc, path, query, fragment = parts
    scheme = "https" if scheme in ("http","https") else scheme
    netloc = netloc.lower()
    fragment = ""  # no usamos #fragment para descargar
    # path limpio, unquote quita cualquier codificacion previa y quote codifica nuevamente, para que quede el patth codificado y no haya por ejemplo una doble codificacion
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
        # le saca a line los espacios del principio y del final
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


# recibe el dict que se le pasa en la funcion principal (la de abajo de todo), y para cada grupo valida hosts con validate_hosts, etc. Escribe un json con esa info de cada url y metadatos
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


def seeds_loader():
    
    diccionario_group_urls: dict[str, list[str]] = {}
    
    # quedan las urls de cada txt en forma lista normalizadas
    urls_langchain = load_seed_file(Path("indexing/crawler/langchain_python.txt"))
    # se arma el diccionario que vamos a transformar en json, con descripcion de las url validas por grupo
    diccionario_group_urls["langchain_python"] = urls_langchain 
    
    
    urls_langsmith = load_seed_file(Path("indexing/crawler/langsmith.txt"))
    diccionario_group_urls["langsmith"] = urls_langsmith
    
    
    urls_langgraph = load_seed_file(Path("indexing/crawler/langgraph.txt"))
    diccionario_group_urls["langgraph"] = urls_langgraph
    
    
    
    # escribe el json en indexing/crawler/seeds_manifest.json
    write_manifest(diccionario_group_urls)
    
    
    
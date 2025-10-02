"""

"""
from __future__ import annotations
from typing import Tuple, List, Dict
from urllib.parse import urlsplit

# ──────────────────────────────────────────────────────────────────────────────
# Configuración de “portería”
# Pensalo simple: primero sacamos lo que seguro NO queremos (assets/ruido),
# después dejamos pasar SOLO las secciones que SÍ queremos por host.
# ──────────────────────────────────────────────────────────────────────────────

ALLOWED_HOSTS = {
    "python.langchain.com",
    "docs.langchain.com",
    "langchain-ai.github.io",
}

# Extensiones que indican archivos estáticos o que no son páginas HTML de interés.
DENY_EXTENSIONS = {
    ".pdf", ".png", ".jpg", ".jpeg", ".svg", ".gif", ".ico",
    ".css", ".js", ".map",
    ".ttf", ".otf", ".woff", ".woff2",
    ".mp4", ".webm", ".mp3",
    ".zip", ".tar", ".gz", ".tgz",
}

# Fragmentos de ruta que suelen ser “ruido” (buscador, sitemaps, recursos de build, etc.)
NOISE_PATH_PARTS = {
    "/search", "/sitemap", "/feed", "/tags", "/category", "/print",
    "/assets/", "/static/", "/_next/", "/__data/",
}

# Lista blanca por host: SOLO estas secciones nos interesan
ALLOW_PREFIXES_BY_HOST = {
    "python.langchain.com": (
        "/docs/concepts/",
        "/docs/how_to/",
        "/docs/tutorials/",
        "/api_reference/",
    ),
    "docs.langchain.com": (
        "/langsmith/",
    ),
    "langchain-ai.github.io": (
        "/langgraph/",
    ),
}

# ──────────────────────────────────────────────────────────────────────────────
# Funciones auxiliares
# ──────────────────────────────────────────────────────────────────────────────

def _has_denied_extension(path: str) -> bool:
    """
    Si la ruta termina con una extensión típica de asset/archivo no-HTML, lo denegamos.
    Ej: /static/logo.svg  → True   |  /docs/how_to/rag/ → False
    """
    # Tomamos el último “segmento” (después del último /) y vemos si tiene punto.
    last = path.rsplit("/", 1)[-1].lower()
    if "." in last:
        # Si hay extensión, probamos contra la lista de negadas.
        for ext in DENY_EXTENSIONS:
            if last.endswith(ext):
                return True
    return False


def _is_noise_path(path: str, query: str) -> bool:
    """
    Detecta rutas que suelen ser ruido (buscador, sitemaps, rutas técnicas).
    También si la query parece de búsqueda (?q=).
    """
    # Chequeo por substrings claros en el path
    for part in NOISE_PATH_PARTS:
        if part in path:
            return True

    # Query de búsqueda simple (?q=)
    # Nota: como ya normalizaste antes, no deberías tener tracking,
    # pero si hay q= lo tomamos como búsqueda y lo saltamos.
    if "q=" in (query or "").lower():
        return True

    return False


def _is_allowed_section(host: str, path: str) -> Tuple[bool, str]:
    """
    Solo dejamos pasar rutas que están en la lista blanca por host.
    Devuelve (True, etiqueta_allow) si matchea; si no, (False, "outside-scope").
    """
    prefixes = ALLOW_PREFIXES_BY_HOST.get(host, ())
    for pref in prefixes:
        if path.startswith(pref):
            # Devolvemos etiqueta corta para el motivo
            # (lo que sigue al último slash sin estar vacío, o el prefijo)
            label = pref.strip("/").split("/")[-1] or pref
            return True, f"ALLOW:{label}"
    return False, "DENY:outside-scope"

# ──────────────────────────────────────────────────────────────────────────────
# funciones principales
# ──────────────────────────────────────────────────────────────────────────────

def allow(url: str) -> Tuple[bool, str]:
    """
    Recibe UNA URL (ya normalizada) y decide si la dejamos pasar o no.
    Devuelve (True/False, motivo corto).

    Orden mental:
      1) Host permitido
      2) Extensión denegada (assets)
      3) Ruido (search/sitemap/static/etc. o ?q=)
      4) Lista blanca por host (secciones que sí queremos)
    """
    parts = urlsplit(url)
    host = parts.netloc.lower()
    path = parts.path or "/"
    query = parts.query or ""

    # 1) Host
    if host not in ALLOWED_HOSTS:
        return False, "DENY:host"

    # 2) Extensión
    if _has_denied_extension(path):
        return False, "DENY:asset"

    # 3) Ruido
    if _is_noise_path(path, query):
        return False, "DENY:noise"

    # 4) Lista blanca por host
    ok, reason = _is_allowed_section(host, path)
    if ok:
        return True, reason

    # Por defecto, no está en el scope que queremos indexar
    return False, "DENY:outside-scope"


def filters(urls: List[str]) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Recibe una lista de URLs (normalizadas) y devuelve:
      - allowed: SOLO las URLs que pasan el filtro.
      - report:  detalle por URL con decisión y motivo (para auditar/ajustar reglas).

    Ejemplo de item en report:
      {"url": "...", "decision": "DENY", "reason": "DENY:asset"}
    """
    allowed: List[str] = []
    report: List[Dict[str, str]] = []

    for u in urls:
        ok, reason = allow(u)
        report.append({
            "url": u,
            "decision": "ALLOW" if ok else "DENY",
            "reason": reason,
        })
        if ok:
            allowed.append(u)

    return allowed, report
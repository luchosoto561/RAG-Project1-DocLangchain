# indexing/crawler/link_extractor.py
from __future__ import annotations
from html.parser import HTMLParser
from pathlib import Path
from typing import List, Set
from urllib.parse import urlsplit, urljoin

import json

# Usamos TU normalizador para que todas las URLs queden con la misma “forma”
# (https, host minúscula, sin #fragment, sin utm_*, con slash final si es carpeta, etc.)
# Si más adelante lo movés a utils/, solo cambiás este import.
from seeds_loader import normalize_url


# ──────────────────────────────────────────────────────────────────────────────
# Coleccionista simple de <a href="..."> usando la librería estándar
# ──────────────────────────────────────────────────────────────────────────────
class _HrefCollector(HTMLParser):
    """
    recorre el HTML y se guarda TODOS los valores de href que
    encuentre en etiquetas <a>. No decide nada; solo junta los textos crudos.
    """

    def __init__(self) -> None:
        # convert_charrefs=True es para que el parser te entregue simbolos raros convertidos ya a la foma "normal"
        super().__init__(convert_charrefs=True)
        # lista donde vamos a almacenar las url
        self.hrefs: List[str] = []

    # funcion que llama el parser cada vez que se encuentra una etiquete de apertura en el html, por ej <a...>, <div...>, etc. tag es el nombre de la etiqueta que acaba de abrir, attrs es una lista de pares (clave, valor) con todos los atributos que venian dentro de esa etiqueta. 
    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.lower() != "a":
            return
        for (k, v) in attrs:
            if k.lower() == "href" and v:
                self.hrefs.append(v.strip())


# ──────────────────────────────────────────────────────────────────────────────
# Funciones auxiliares de descarte y chequeos rápidos
# ──────────────────────────────────────────────────────────────────────────────
def _is_trivial_or_anchor(href: str) -> bool:
    """
    Acá filtramos lo que no suma nada: vacío, "#", "   ", etc, es decir, chequea si el link no lleva a ningun lado util
    """
    s = href.strip()
    # verdadero si s esta vacio o s es #, indicando que el link no nos importa
    return (not s) or (s == "#")


def _is_non_web_scheme(href: str) -> bool:
    """
    Si el link es de “otro mundo” (mailto:, tel:, javascript:, data:),
    lo descartamos porque no es una página web para descargar.
    """
    s = href.strip().lower()
    return s.startswith(("mailto:", "tel:", "javascript:", "data:"))


def _read_html(file_path: str) -> str:
    """
    Lee el HTML crudo desde disco y te lo devuelve como texto.
    """
    return Path(file_path).read_text(encoding="utf-8", errors="ignore")


# ──────────────────────────────────────────────────────────────────────────────
# Función principal: sacar links internos útiles de un HTML ya descargado
# ──────────────────────────────────────────────────────────────────────────────
def helper_link_extractor(file_path: str, base_url: str, allowed_hosts: Set[str],) -> List[str]:
    """
    - Abrimos el HTML que ya tenés guardado.
    - Juntamos TODOS los href de <a>.
    - Limpiamos lo obvio (vacíos, #, mailto:, etc.).
    - Convertimos a URLs absolutas usando base_url (para que “../algo” quede completo).
    - Pasamos cada URL por TU normalize_url (para que queden uniformes).
    - Nos quedamos SOLO con las que son del/los dominios oficiales (allowed_hosts).
    - Sacamos duplicados triviales manteniendo el orden de aparición.
    - Devolvemos la lista final para que el crawler la procese como candidatas.
    """

    # 1) Leemos el HTML
    html = _read_html(file_path)

    # 2) Extraemos todos los href “crudos”
    parser = _HrefCollector()
    parser.feed(html)
    raw_hrefs = parser.hrefs

    # 3) Procesamos y filtramos
    out: List[str] = []
    seen: Set[str] = set()

    base_host = urlsplit(base_url).netloc.lower()

    for href in raw_hrefs:
        # 3.a) Saltamos lo vacío, “#”, etc.
        if _is_trivial_or_anchor(href):
            continue

        # 3.b) Saltamos esquemas no web
        if _is_non_web_scheme(href):
            continue

        # 3.c) Resolvemos relativos con la base (si href es absoluto, urljoin lo deja igual)
        #      Ejemplo: "../guide/" + base "https://site/docs/x/" → "https://site/docs/guide/"
        abs_url = urljoin(base_url, href)

        # 3.d) Normalizamos con TU función (https, host minúscula, sin fragment, sin utm, etc.)
        try:
            norm = normalize_url(abs_url, base=None)
        except Exception:
            # Si por alguna razón una URL está mal formada, la saltamos y seguimos
            continue

        # 3.e) Solo URLs internas (dominios oficiales)
        host = urlsplit(norm).netloc.lower()
        if host not in allowed_hosts:
            # Permitimos también sub-pages del mismo host base_url si lo preferís,
            # pero en este diseño exigimos que esté en allowed_hosts (más seguro).
            continue

        # 3.f) Evitamos duplicados triviales manteniendo el orden de aparición
        if norm in seen:
            continue
        seen.add(norm)
        out.append(norm)

    return out

ALLOWED_HOSTS = {
    "python.langchain.com",
    "docs.langchain.com",
    "langchain-ai.github.io",
}


def link_extractor(index_path: Path = Path("data/raw_pages/index.json"), allowed_hosts: set[str] = ALLOWED_HOSTS) -> list[str]:
    """
    - Lee el index.json (array) que generaste al bajar las seeds (profundidad 0).
    - Recorre SOLO los registros OK (status_code == 200) que tienen ruta al HTML.
    - Para cada HTML, usa extract_internal_links(...) para sacar los links internos.
    - Junta todos los links en UNA lista, sin duplicados y manteniendo el orden.
    - Devuelve esa lista final (candidatas a profundidad 1).

    No escribe archivos, no hace red. Devuelve list[str].
    """
    if not index_path.exists():
        return []

    try:
        records = json.loads(index_path.read_text(encoding="utf-8"))
        if not isinstance(records, list):
            # por si alguien guardó JSONL por error; en ese caso devolvemos vacío
            return []
    except Exception:
        return []

    seen: set[str] = set()
    all_links: list[str] = []

    for rec in records:
        if not isinstance(rec, dict):
            continue

        status = rec.get("status_code")
        file_path = rec.get("html_crudo_path")
        base_url  = rec.get("url")

        # Solo páginas que realmente bajaste y son HTML 200
        if status != 200 or not file_path or not base_url:
            continue

        try:
            links = helper_link_extractor(file_path=str(Path(file_path)), base_url=str(base_url), allowed_hosts=allowed_hosts,)
        except Exception:
            # si una página puntual está rota, seguimos con las demás
            continue

        # dedup preservando orden de primera aparición global
        for u in links:
            if u in seen:
                continue
            seen.add(u)
            all_links.append(u)

    return all_links
    
    
    
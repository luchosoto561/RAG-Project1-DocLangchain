#!/usr/bin/env python3
"""
Crawler v1 para RAG-Langchain (solo: LangChain Python, LangGraph y LangSmith).
- Lee seeds desde config/seeds/*.txt
- Respeta robots.txt, sigue solo enlaces internos y bajo prefijos permitidos
- Normaliza URLs (https, sin #fragment, sin utm/ref, trailing slash consistente)
- Descarga HTML crudo y lo guarda en data/raw_pages/<host>/<YYYYMMDD>/<page_id>.html
- Escribe un index.jsonl con metadatos básicos por página

Requisitos:
    pip install httpx beautifulsoup4

Uso:
    python indexing/crawler.py --seeds-dir config/seeds --max-depth 2 --rate 1.0

Nota: Mantener simple y auditable. El parser/chunker vendrán luego.
"""
from __future__ import annotations

import asyncio
import dataclasses
import datetime as dt
import hashlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse, urlsplit, urlunsplit, quote, unquote
import urllib.robotparser as robotparser

import httpx
from bs4 import BeautifulSoup

# ---------------------------
# Configuración / Defaults
# ---------------------------
ALLOW_TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid", "ref"}
DENY_EXTENSIONS = {".png", ".jpg", ".jpeg", ".svg", ".gif", ".pdf", ".ico", ".webp"}
DEFAULT_TIMEOUT = 15.0
DEFAULT_RETRIES = 2
DEFAULT_RATE = 1.0  # req/seg por host
USER_AGENT = "RAG-Langchain-Crawler/0.1 (+contact)"

# ---------------------------
# Utilidades
# ---------------------------

def _today_stamp() -> str:
    return dt.datetime.now().strftime("%Y%m%d")


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def strip_tracking_query(url: str) -> str:
    parts = urlsplit(url)
    if not parts.query:
        return url
    # filtrar parámetros no esenciales
    kept = []
    for kv in parts.query.split("&"):
        if not kv:
            continue
        k = kv.split("=", 1)[0]
        if k in ALLOW_TRACKING_PARAMS:
            continue
        kept.append(kv)
    new_query = "&".join(kept)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def normalize_url(url: str, base: Optional[str] = None) -> str:
    """Normaliza una URL para *fetching* (sin fragmento), https, host minúsculas, sin tracking.
    No resuelve canónica por <link rel="canonical"> aquí (esa la detectamos tras bajar el HTML).
    """
    if base:
        url = urljoin(base, url)
    url = strip_tracking_query(url)
    parts = list(urlsplit(url))
    # forzar https si no está
    if parts[0] in ("http", "https"):
        parts[0] = "https"
    # host minúsculas
    parts[1] = parts[1].lower()
    # quitar fragmento
    parts[4] = ""
    # normalizar path (decodificar y re-encode limpio)
    path = unquote(parts[2])
    # trailing slash consistente para paths que parecen carpeta
    if not os.path.splitext(path)[1] and not path.endswith("/"):
        path = path + "/"
    parts[2] = quote(path)
    return urlunsplit(tuple(parts))


def same_host(u1: str, u2: str) -> bool:
    return urlsplit(u1).netloc == urlsplit(u2).netloc


def has_denied_extension(url: str) -> bool:
    path = urlsplit(url).path
    _, ext = os.path.splitext(path)
    return ext.lower() in DENY_EXTENSIONS


def page_id_for(url_canonica: str) -> str:
    return sha1(url_canonica)


# ---------------------------
# Dataclasses de configuración
# ---------------------------
@dataclasses.dataclass
class SeedGroup:
    name: str  # ej. "langchain_python", "langgraph", "langsmith"
    urls: List[str]

    @property
    def hosts(self) -> Set[str]:
        return {urlsplit(u).netloc for u in self.urls}

    @property
    def allow_prefixes(self) -> Dict[str, Set[str]]:
        """Prefijos de path permitidos por host, derivados de las seeds (simple y seguro)."""
        per_host: Dict[str, Set[str]] = {}
        for u in self.urls:
            parts = urlsplit(u)
            per_host.setdefault(parts.netloc, set()).add(parts.path)
        return per_host


@dataclasses.dataclass
class CrawlConfig:
    seeds_dir: Path
    out_dir: Path = Path("data/raw_pages")
    index_path: Path = Path("data/raw_pages/index.jsonl")
    manifest_path: Path = Path("data/manifests/manifest.json")
    max_depth: int = 2
    rate_per_host: float = DEFAULT_RATE
    timeout: float = DEFAULT_TIMEOUT
    retries: int = DEFAULT_RETRIES


# ---------------------------
# Robots cache
# ---------------------------
class RobotsCache:
    def __init__(self):
        self._cache: Dict[str, robotparser.RobotFileParser] = {}

    async def allowed(self, client: httpx.AsyncClient, url: str) -> bool:
        parts = urlsplit(url)
        host = parts.netloc
        if host not in self._cache:
            robots_url = urlunsplit((parts.scheme, parts.netloc, "/robots.txt", "", ""))
            rp = robotparser.RobotFileParser()
            try:
                r = await client.get(robots_url, headers={"User-Agent": USER_AGENT}, timeout=DEFAULT_TIMEOUT)
                if r.status_code == 200:
                    rp.parse(r.text.splitlines())
                else:
                    # si no hay robots.txt, permitir por defecto
                    rp.parse(["User-agent: *", "Allow: /"])
            except Exception:
                rp.parse(["User-agent: *", "Allow: /"])
            self._cache[host] = rp
        return self._cache[host].can_fetch(USER_AGENT, url)


# ---------------------------
# Crawler
# ---------------------------
class Crawler:
    def __init__(self, cfg: CrawlConfig, groups: List[SeedGroup]):
        self.cfg = cfg
        self.groups = groups
        self.visited: Set[str] = set()  # URLs canónicas visitadas
        self.host_rate: Dict[str, float] = {}  # próximos timestamps permitidos por host
        ensure_dir(self.cfg.out_dir)
        ensure_dir(self.cfg.index_path.parent)
        ensure_dir(self.cfg.manifest_path.parent)
        self._index_fh = open(self.cfg.index_path, "a", encoding="utf-8")
        self.robots = RobotsCache()
        self.today = _today_stamp()

    # -----------------------
    # Rate limiting por host
    # -----------------------
    async def _respect_rate(self, url: str):
        host = urlsplit(url).netloc
        now = dt.datetime.now().timestamp()
        next_allowed = self.host_rate.get(host, 0.0)
        delay = max(0.0, next_allowed - now)
        if delay > 0:
            await asyncio.sleep(delay)
        # set próximo instante permitido
        self.host_rate[host] = dt.datetime.now().timestamp() + (1.0 / max(self.cfg.rate_per_host, 1e-6))

    # -----------------------
    # Escribir index.jsonl
    # -----------------------
    def _write_index(self, record: dict):
        self._index_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._index_fh.flush()

    # -----------------------
    # Lógica principal
    # -----------------------
    async def run(self):
        started = dt.datetime.now().isoformat()
        total_downloaded = 0
        totals = {"downloaded": 0, "skipped": 0, "failed": 0}

        async with httpx.AsyncClient(follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
            # construir colas por grupo
            queue: List[Tuple[str, int, Optional[str], str, SeedGroup]] = []
            for g in self.groups:
                for u in g.urls:
                    nu = normalize_url(u)
                    queue.append((nu, 0, None, g.name, g))

            while queue:
                url, depth, parent, group_name, group = queue.pop(0)
                # filtros básicos
                if has_denied_extension(url):
                    totals["skipped"] += 1
                    continue
                if url in self.visited:
                    totals["skipped"] += 1
                    continue

                # permitir solo hosts del grupo
                host = urlsplit(url).netloc
                if host not in group.hosts:
                    totals["skipped"] += 1
                    continue

                # prefijos permitidos (derivados de seeds)
                path = urlsplit(url).path
                allow_ok = any(path.startswith(p) for p in group.allow_prefixes.get(host, {"/"}))
                if not allow_ok:
                    totals["skipped"] += 1
                    continue

                # robots
                if not await self.robots.allowed(client, url):
                    self._write_index({
                        "url_solicitada": url,
                        "url_canonica": url,
                        "host": host,
                        "depth": depth,
                        "status_code": None,
                        "fetched_at": dt.datetime.now().isoformat(),
                        "html_crudo_path": None,
                        "seed_group": group_name,
                        "parent_url": parent,
                        "robots_allowed": False,
                        "out_links_count": 0,
                        "error_type": "robots"
                    })
                    totals["skipped"] += 1
                    continue

                # rate limit
                await self._respect_rate(url)

                # fetch con reintentos
                resp = None
                for attempt in range(self.cfg.retries + 1):
                    try:
                        resp = await client.get(url, timeout=self.cfg.timeout)
                        break
                    except httpx.RequestError:
                        if attempt < self.cfg.retries:
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                        else:
                            resp = None
                            break

                fetched_at = dt.datetime.now().isoformat()
                if resp is None:
                    self._write_index({
                        "url_solicitada": url,
                        "url_canonica": url,
                        "host": host,
                        "depth": depth,
                        "status_code": None,
                        "fetched_at": fetched_at,
                        "html_crudo_path": None,
                        "seed_group": group_name,
                        "parent_url": parent,
                        "robots_allowed": True,
                        "out_links_count": 0,
                        "error_type": "timeout_or_network"
                    })
                    totals["failed"] += 1
                    continue

                # solo HTML
                ctype = resp.headers.get("Content-Type", "").lower()
                if "text/html" not in ctype:
                    self._write_index({
                        "url_solicitada": url,
                        "url_canonica": url,
                        "host": host,
                        "depth": depth,
                        "status_code": resp.status_code,
                        "fetched_at": fetched_at,
                        "html_crudo_path": None,
                        "seed_group": group_name,
                        "parent_url": parent,
                        "robots_allowed": True,
                        "out_links_count": 0,
                        "error_type": "non_html"
                    })
                    totals["skipped"] += 1
                    continue

                # detectar canónica si existe
                final_url = str(resp.request.url)
                soup = BeautifulSoup(resp.text, "html.parser")
                canon = soup.find("link", rel=lambda v: v and "canonical" in v)
                url_canonica = normalize_url(canon.get("href"), base=final_url) if canon and canon.get("href") else normalize_url(final_url)

                # evitar salir del host por canónica
                if not same_host(url_canonica, final_url):
                    url_canonica = normalize_url(final_url)

                if url_canonica in self.visited:
                    totals["skipped"] += 1
                    continue
                self.visited.add(url_canonica)

                # guardar HTML crudo
                pid = page_id_for(url_canonica)
                host_dir = Path(self.cfg.out_dir) / urlsplit(url_canonica).netloc / self.today
                ensure_dir(host_dir)
                html_path = host_dir / f"{pid}.html"
                html_path.write_text(resp.text, encoding="utf-8")

                # extraer enlaces internos para expandir (si depth < max)
                out_links = 0
                if depth < self.cfg.max_depth:
                    for a in soup.find_all("a", href=True):
                        href = a.get("href")
                        new_url = normalize_url(href, base=final_url)
                        if not new_url:
                            continue
                        if has_denied_extension(new_url):
                            continue
                        # mismo host y bajo prefijos permitidos del grupo
                        if same_host(new_url, final_url):
                            new_path = urlsplit(new_url).path
                            if any(new_path.startswith(p) for p in group.allow_prefixes.get(host, {"/"})):
                                if new_url not in self.visited:
                                    queue.append((new_url, depth + 1, url_canonica, group_name, group))
                                    out_links += 1

                # escribir registro en index.jsonl
                self._write_index({
                    "page_id": pid,
                    "url_solicitada": url,
                    "url_canonica": url_canonica,
                    "host": host,
                    "depth": depth,
                    "status_code": resp.status_code,
                    "content_type": ctype,
                    "fetched_at": fetched_at,
                    "html_crudo_path": str(html_path),
                    "seed_group": group_name,
                    "parent_url": parent,
                    "robots_allowed": True,
                    "out_links_count": out_links,
                    "error_type": None,
                })
                totals["downloaded"] += 1

        # manifest
        finished = dt.datetime.now().isoformat()
        manifest = {
            "started_at": started,
            "finished_at": finished,
            "seeds_dir": str(self.cfg.seeds_dir),
            "max_depth": self.cfg.max_depth,
            "rate_per_host": self.cfg.rate_per_host,
            "timeout": self.cfg.timeout,
            "retries": self.cfg.retries,
            "totals": totals,
        }
        Path(self.cfg.manifest_path).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        self._index_fh.close()


# ---------------------------
# Helpers para cargar seeds
# ---------------------------
SEED_FILES = {
    "langchain_python": "langchain_python.txt",
    "langgraph": "langgraph.txt",
    "langsmith": "langsmith.txt",
}


def load_seed_groups(seeds_dir: Path) -> List[SeedGroup]:
    groups: List[SeedGroup] = []
    for name, fname in SEED_FILES.items():
        p = seeds_dir / fname
        if not p.exists():
            continue
        urls: List[str] = []
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(normalize_url(line))
        if urls:
            groups.append(SeedGroup(name=name, urls=urls))
    return groups


# ---------------------------
# CLI
# ---------------------------
async def _amain():
    import argparse

    parser = argparse.ArgumentParser(description="Crawler v1 para RAG-Langchain")
    parser.add_argument("--seeds-dir", type=Path, default=Path("config/seeds"))
    parser.add_argument("--max-depth", type=int, default=2)
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE, help="req/seg por host")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    args = parser.parse_args()

    cfg = CrawlConfig(
        seeds_dir=args.seeds_dir,
        max_depth=args.max_depth,
        rate_per_host=args.rate,
        timeout=args.timeout,
        retries=args.retries,
    )

    groups = load_seed_groups(cfg.seeds_dir)
    if not groups:
        print(f"No se encontraron seeds en {cfg.seeds_dir}", file=sys.stderr)
        sys.exit(1)

    crawler = Crawler(cfg, groups)
    await crawler.run()
    print("✔ Crawl terminado. Ver data/raw_pages y data/raw_pages/index.jsonl")


def main():
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        print("Interrumpido por el usuario")


if __name__ == "__main__":
    main()

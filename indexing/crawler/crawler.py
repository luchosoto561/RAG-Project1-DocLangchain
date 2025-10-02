"""
orquestador del crawler completo.
objetivo: Recolectar de forma controlada y reproducible el HTML crudo de la documentacion oficial de Langchain/LangGraph/Langsmith,
siguiendo solo enlaces internos relevantes hasta una profundidad acotada, respetando robots, con ritmo seguro y sin descargas repetidas.
El resultado es un conjunto de paginas crudas + un grafo de navegacion que servira de base para el parser, chunker, metadatos y deduplicacion
de contenido en la etapa de indexing.

Alcance:
- Arranca desde seeds_loader creando seeds_manifest.json, con las url validas limpias
- Sigue links internos permitidos
- Evita deduplicados en URLs
- Respeta rate-limit y aplica backoff ante 429/5xx
"""
from __future__ import annotations
from pathlib import Path
from typing import Optional
import json
import time
from urllib.parse import urlsplit

from seeds_loader import seeds_loader # funcion que crea el seeds_manifest.json con las url limpias e info de las mismas

from fetcher_v1 import fetcher_v1 # funcion que descarga las urls sin repetidas y crea json con info de las descargas
from fetcher_v1 import fetch_and_save # funcion para descargar el html

from link_extractor import link_extractor # funcion que te devuelve los links dentro de un html, sin repetidos y normalizados

from rate_limit_backoff import fetch_with_rate_limit_and_backoff # funcion que maneja la descarga de una url, para que no te bloquee el back de la pagina donde lo estas descargando

seeds_loader()

fetcher_v1()

urls_internas_candidatas = link_extractor()


for url in urls_internas_candidatas:
    fetch_with_rate_limit_and_backoff(url, fetch_and_save)
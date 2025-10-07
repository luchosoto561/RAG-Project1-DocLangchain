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

from seeds_loader import seeds_loader 

from fetcher_v1 import fetcher_v1 
from fetcher_v1 import fetch_and_save # funcion para descargar el html

from link_extractor import link_extractor 

from rate_limit_backoff import fetch_with_rate_limit_and_backoff # funcion que maneja la descarga de una url, para que no te bloquee el back de la pagina donde lo estas descargando

from filters import filters

from fetcher_v1 import Dict_por_url
from fetcher_v1 import append_index

# crea el seeds_manifest.json con las urls limpias e info de las mismas
seeds_loader()

# descarga las urls sin repetidas y crea json con info de las descargasindex.jsonl
fetcher_v1()

# urls_internas_candidatas tine los links dentro de un html, sin repetidos y normalizados
urls_internas_candidatas = link_extractor()

allowed, report = filters(urls_internas_candidatas)
print(urls_internas_candidatas)

print("vamos a descargar las siguientes urls que son las que pasaron por los filtros\n")

print(allowed)

print("a continuacion se imprime el reporte:\n")
print(report)

# La “lista grande” de registros:
lista_diccionarios_por_url: list[Dict_por_url] = []

for url in allowed:     
    lista_diccionarios_por_url.append(fetch_with_rate_limit_and_backoff(url, fetch_and_save))
    append_index(lista_diccionarios_por_url)


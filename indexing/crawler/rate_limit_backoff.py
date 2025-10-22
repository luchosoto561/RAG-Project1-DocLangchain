"""
Objetivo del archivo -> 
- marcar el ritmo por host (rate - limit). Ej: como maximo una request por segundo a python.langchain.com
para no recibir 429.
- Reintentar con PAUSAS CRECIENTES (backoff) cuando hay 429/5xx/errores de red.
"""
from __future__ import annotations
import time 
import random
from typing import Callable, Dict, Any, Tuple
from urllib.parse import urlsplit

# ultimo momento en que golpeamos cada host
_LAST_HIT_PER_HOST: Dict[str, float] = {}

# parametros por defecto (ajustables)
DEFAULT_MIN_INTERVAL_S = 1.0 # 1 request por segundo por host
DEFAULT_MAX_RETRIES = 3 # cantidad de reintentos ante 429/5xx/errores
DEFAULT_BACKOFF_BASE_S = 1.0 # base del backoff exponencial (1s, 2s, 4s, ...)
DEFAULT_JITTER_FACTOR = 0.5 # +/-50% de aleatorio para evitar sincronizarnos

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _host_from_url(url: str) -> str:
    """
    Saca el host de la URL (ej: 'python.langchain.com').
    
    """
    return urlsplit(url).netloc.lower()

def _rate_limit_wait(host: str, min_interval_s: float = DEFAULT_MIN_INTERVAL_S) -> float:
    """
    Si a este host le pegamos hace muy poquito, esperamos lo que falta
    para respetar el intervalo mínimo. Devolvemos cuántos segundos dormimos.
    
    """
    now = time.monotonic() # pide un reloj que solo avanza (no se ve afectado por cambios de hora del sistema), lo usamos para medir intervalos con confianza
    last = _LAST_HIT_PER_HOST.get(host, 0.0) # miramos cuando fue la ultima vez que le pegamos a ese host, si no le pegamos nunca devuelve 0.0 por defecto
    elapsed = now - last # calculamos cuantos segundos pasaron desde el ultimo hit a ese host
    wait_s = max(0.0, min_interval_s - elapsed) # funcion nativa de python si la resta es negativa devuelve 0.0, sino devuelve la resta
    if wait_s > 0:
        time.sleep(wait_s)
    # registramos que a partir de AHORA le estamos por pegar al host
    _LAST_HIT_PER_HOST[host] = time.monotonic()
    return wait_s


def _backoff_delay_s(attempt: int, # numero de reintento
                     base_s: float = DEFAULT_BACKOFF_BASE_S, # base del backoff en segundos
                     jitter_factor: float = DEFAULT_JITTER_FACTOR # cuanta variasion aleatoria permitimos alrededor del ideal
                     ) -> float:
    """
    Calcula cuanto dormir antes de reintentar una descarga que fallo. Usa backoff exponencial con jitter (aleatorio), el backoff exponencial
    quiere decir que cada reintento espera mas que el anterior, esto baja la presion sobre el servidor y suele dar tiempo a que se recupere. 
    Por otro lado jitter es la variacion aleatoria alrededor del tiempo que ibas a esperar. Por ejemplo si tu delay especial es 4s, con jitter podes
    esperar algo entre 2s y 6s; esto se usa para evitar que muchos clientes reintenten todos al mismo tiempo.  
    
    """
    ideal = base_s * (2 ** attempt)
    # Jitter: multiplicamos por un factor entre (1 - j) y (1 + j)
    low = 1.0 - jitter_factor
    high = 1.0 + jitter_factor
    return ideal * random.uniform(low, high)


def _should_retry(rec: Dict[str, Any]) -> bool:
    """
    Decidimos si vale la pena reintentar según el resultado de la descarga.
    - 429 (Too Many Requests) → sí
    - 5xx (errores del server) → sí
    - errores de red/timeout (tu función suele ponerlos en rec["error"]) → sí
    - 4xx distintos de 429 (404, 401, 410, etc.) → no conviene reintentar
    - 200 y además guardó HTML → no reintentar
    """
    status = rec.get("status_code")
    html_path = rec.get("html_crudo_path")
    error = (rec.get("error") or "") if isinstance(rec.get("error"), str) else ""

    # éxito claro: 200 + archivo guardado
    if status == 200 and html_path:
        return False

    # 429: nos están rate-limiteando
    if status == 429:
        return True

    # 5xx: suelen ser transitorios
    if isinstance(status, int) and 500 <= status <= 599:
        return True

    # errores de red (según cómo los registre tu fetcher)
    # ej: "network:ReadTimeout" / "network:ConnectError"
    if error.startswith("network:"):
        return True

    return False

# ──────────────────────────────────────────────────────────────────────────────
# API principal
# ──────────────────────────────────────────────────────────────────────────────

def fetch_with_rate_limit_and_backoff(
    url: str, # la URL a descargar
    fetch_fn: Callable[[str], Dict[str, Any]], # funcion que vos le pasas que hace la descarga, recibe un string y devuelve un diccionario con el resultado.
    *,
    min_interval_s: float = DEFAULT_MIN_INTERVAL_S, # intervalo minimo que se espera entre request
    max_retries: int = DEFAULT_MAX_RETRIES, # maximo de intentos
    backoff_base_s: float = DEFAULT_BACKOFF_BASE_S, # marca el primer escalon de espera cuando tenes que reintentar DEFAULT_BACKOFF_BASE_S * 2 ** 0
    jitter_factor: float = DEFAULT_JITTER_FACTOR, # porcentaje de variacion aleatoria que aplicamos a ese tiempo ideal para desincronizar reintentos, evitando que todas las request se reintenten al mismo tiempo
) -> Dict[str, Any]:
    """
    - Espera lo necesario para no "rafaguear" al host (rate-limit por host).
    - Llama a tu fetch_and_save(url).
    - Si la respuesta sugiere reintentar (429/5xx/network), espera con backoff y reintenta,
      hasta max_retries.
    - Devuelve el registro final (el de tu fetcher), agregándole algunos campos útiles:

        rec["rl_wait_s_total"]  → cuánto durmió por rate-limit en total
        rec["retries"]          → cuántos reintentos hizo (0 = salió a la 1ra)
        rec["last_backoff_s"]   → cuánto fue la última espera de backoff (si hubo)

    Notas:
    - No necesitamos modificar tu fetcher. Este wrapper decide CUÁNDO llamar y CUÁNDO reintentar.
    - Si en el futuro querés respetar "Retry-After", tendríamos que exponer headers desde fetch_fn.
    """
    host = _host_from_url(url)
    # cuanto dormimos en total por ritmo, sirve para info (metricas) pero no afecta la logica
    total_rl_wait = 0.0
    # cuanto dormimos en el ultimo backoff (si hubo), sirve para info (metricas) pero no afecta la logica
    last_backoff = 0.0

    # contador de reintentos
    attempt = 0
    
    # porque no sabemos cuantos intentos vamos a necesitar para descargar, salimos con break dentro del while
    while True:
        # 1) Ritmo por host
        waited = _rate_limit_wait(host, min_interval_s=min_interval_s)
        total_rl_wait += waited

        # 2) Intento de descarga
        rec = fetch_fn(url)

        # 3) ¿Hace falta reintentar?
        if not _should_retry(rec):
            # Éxito o fallo "definitivo" (ej. 404): salimos y devolvemos lo que hay
            break

        if attempt >= max_retries:
            # Ya usamos todos los reintentos. Marcamos y salimos.
            rec.setdefault("error", None)
            if not rec["error"]:
                rec["error"] = f"retry_exhausted_after_{max_retries}"
            break

        # 4) Backoff antes del próximo intento
        last_backoff = _backoff_delay_s(attempt, base_s=backoff_base_s, jitter_factor=jitter_factor)
        time.sleep(last_backoff)
        attempt += 1

    # Campos de diagnóstico (no rompen tu esquema; solo agregan info)
    rec["rl_wait_s_total"] = round(total_rl_wait, 3)
    rec["retries"] = attempt  # 0 si salió al primer intento
    rec["last_backoff_s"] = round(last_backoff, 3)

    return rec

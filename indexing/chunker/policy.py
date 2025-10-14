# chunker/policy.py
"""
Reglas mínimas del chunker v1
Este módulo NO transforma contenido ni conoce la estructura del documento.
Sólo expone:
- Presupuesto de tamaño en tokens (soft/hard) para decidir cortes.
- Bandera para incluir el heading (título de sección) al inicio del chunk.
- Una estimación de tokens simple, barata y determinista.

Cualquier ajuste global al tamaño de los chunks (v1) se hace cambiando aquí
SOFT_TOKENS / HARD_TOKENS y volviendo a correr el indexado.
"""

from math import ceil

# -----------------------------------------------------------------------------
# Presupuesto de tamaño (en tokens aprox.)
# -----------------------------------------------------------------------------
# SOFT_TOKENS: objetivo. Si lo superamos un poco pero aún estamos por debajo
# del HARD_TOKENS, el chunk sigue siendo válido (no cortamos de forma agresiva).
SOFT_TOKENS: int = 700

# HARD_TOKENS: límite absoluto. Si agregar el siguiente bloque/subsección
# superaría este valor, se corta ANTES de ese agregado y se deja para el próximo chunk.
HARD_TOKENS: int = 1000

# -----------------------------------------------------------------------------
# Política de encabezado en el texto del chunk
# -----------------------------------------------------------------------------
# INCLUDE_HEADING: si True, el core antepone el heading_text (H2/H3 que define
# el chunk) al inicio del texto. Mejora contexto semántico y alinea con el anchor
# de la cita. Si False, el texto del chunk comienza directamente por los bloques.
INCLUDE_HEADING: bool = True

# -----------------------------------------------------------------------------
# Estimación de tokens
# -----------------------------------------------------------------------------
def estimate_tokens(text: str) -> int:
    """
    Estima el recuento de tokens de forma barata y determinista para comparar
    contra SOFT_TOKENS / HARD_TOKENS. NO busca exactitud perfecta; el objetivo
    es ser consistente en decisiones de "entra / no entra" durante el armado.

    Heurística utilizada:
      ~4 caracteres por token en textos técnicos (inglés/español).
    Motivos:
      - Costo constante (O(1) respecto a la longitud del string, sin splits).
      - Sin dependencias externas ni variabilidad.
      - Suficiente para v1 (no re-normalizamos; tomamos el string tal cual).

    Args:
        text: Texto completo del chunk (ya renderizado por core, sin alteraciones).

    Returns:
        int: Cantidad aproximada de tokens (redondeo hacia arriba para no subestimar).
    """
    # Longitud en caracteres dividido por 4, con techo para evitar subestimación.
    # Esta cota favorece decisiones conservadoras frente a SOFT/HARD.
    return ceil(len(text) / 4)


# Exportaciones explícitas (opcional, deja claro el contrato público del módulo)
__all__ = [
    "SOFT_TOKENS",
    "HARD_TOKENS",
    "INCLUDE_HEADING",
    "estimate_tokens",
]

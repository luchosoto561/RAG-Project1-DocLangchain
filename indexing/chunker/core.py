# chunker/core.py
"""
Chunker v1: toma una página parseada y devuelve una lista de chunks listos
para indexar. Respeta:
- Unidad base H2 (si no hay H2, usa H1/raíz).
- Incluye H3 hijos mientras no exceda presupuesto; si no entra, divide por H3.
- Nunca corta dentro de bloques de código.
- No re-normaliza: usa el texto tal cual viene del parser.
- Ancla las citas a H2 o H3 según corresponda; si falta anchor, sube al padre.
- Emite metadatos mínimos e intenta deduplicar.

Contrato de entrada esperado (resumen):
parsed_page = {
  "provenance": {"url_final": str, "title": str, "fetched_at": str},
  "sections": [  # árbol de secciones
    {
      "level": 1|2|3,
      "heading_text": str | None,
      "anchor": str | None,         # id de la sección en el HTML, si existe
      "blocks": [                    # lista ordenada
        {"type": "paragraph", "text": str}
        {"type": "list", "items": [str, ...]}
        {"type": "code", "language": str|None, "content": str}
      ],
      "children": [ ... mismas keys ... ]
    },
    ...
  ]
}

Salida (cada chunk es un dict JSON-serializable):
{
  "id": str,                        # sha1 estable
  "text": str,                      # concatenado tal cual del parser
  "url_final": str,
  "url_citable": str,               # url_final o url_final#anchor
  "title": str,
  "section_level": int,             # 1/2/3
  "section_heading": str | None,
  "section_anchor": str | None,     # anchor real elegido (si existe)
  "fetched_at": str,
  "has_code": bool,
}
"""

from __future__ import annotations
from typing import Dict, Any, Iterable, List, Optional, Tuple
import hashlib

from policy import SOFT_TOKENS, HARD_TOKENS, INCLUDE_HEADING, estimate_tokens


# ------------------------------- helpers de árbol -------------------------------

def _iter_h2_nodes(sections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Devuelve todas las secciones nivel 2 (H2) en orden de aparición."""
    out: List[Dict[str, Any]] = []
    for s in sections:
        lvl = s.get("level")
        if lvl == 2:
            out.append(s)
        elif lvl == 1:
            # H2 suelen estar dentro del H1
            out.extend(_iter_h2_nodes(s.get("children", [])))
        else:
            # nivel 3 u otros al tope no se consideran raíz de H2
            pass
    return out


def _first_h1_or_root(sections: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Devuelve la sección H1 si existe; si no, crea una raíz sintética a partir del tope."""
    for s in sections:
        if s.get("level") == 1:
            return s
    # Raíz sintética: todo lo que esté al tope se considera “contenido de raíz”
    return {
        "level": 1,
        "heading_text": None,
        "anchor": None,
        "blocks": [],
        "children": sections,  # consideramos que el contenido vive abajo
    }


# ------------------------------- renderizado -----------------------------------

def _render_block(block: Dict[str, Any]) -> Tuple[str, bool]:
    """
    Convierte un bloque a texto. Devuelve (texto, has_code_en_este_bloque).
    No normaliza ni re-escribe; mantiene el contenido literal.
    """
    btype = block.get("type")
    if btype == "paragraph":
        return (block.get("text", ""), False)
    if btype == "list":
        items = block.get("items", []) or []
        # Mantener formato simple de lista; cada item en su línea.
        return ("\n".join(f"- {str(it)}" for it in items), False)
    if btype == "code":
        lang = block.get("language") or ""
        code = block.get("content", "")
        fence = f"```{lang}\n{code}\n```" if lang else f"```\n{code}\n```"
        return (fence, True)
    # Desconocido: lo ignoramos de forma segura.
    return ("", False)


def _render_blocks(blocks: List[Dict[str, Any]]) -> Tuple[str, bool]:
    """
    Concatena bloques en orden. Devuelve (texto, has_code_en_algún_bloque).
    Usa una línea en blanco entre bloques para legibilidad mínima.
    """
    parts: List[str] = []
    has_code = False
    for b in blocks or []:
        t, hc = _render_block(b)
        if t:
            parts.append(t)
        has_code = has_code or hc
    # Separación suave entre bloques
    text = "\n\n".join(parts).strip()
    return (text, has_code)


def _render_heading_line(node: Dict[str, Any]) -> str:
    """Devuelve la línea de heading si existe; de lo contrario, vacío."""
    h = node.get("heading_text")
    return h.strip() if isinstance(h, str) and h.strip() else ""


def _concat_heading_and_body(heading_line: str, body: str) -> str:
    """Si INCLUDE_HEADING, antepone el heading como primera línea; si no, devuelve solo el body."""
    if not body:
        # Si no hay cuerpo, igual podemos devolver solo el heading (contexto)
        return heading_line if INCLUDE_HEADING else ""
    if INCLUDE_HEADING and heading_line:
        # Heading, luego una línea en blanco, luego el cuerpo
        return f"{heading_line}\n\n{body}"
    return body


# ------------------------------- anchors ---------------------------------------

def _ascend_anchor(node: Dict[str, Any], parent_chain: List[Dict[str, Any]]) -> Optional[str]:
    """
    Devuelve el primer anchor disponible ascendiendo: node → padres.
    Si ninguno tiene anchor, retorna None (url_citable será la url_final sin #).
    """
    if node.get("anchor"):
        return node.get("anchor")
    for p in reversed(parent_chain):
        if p.get("anchor"):
            return p.get("anchor")
    return None


# ---------------------------- ids y deduplicación ------------------------------
# para deduplicar, es el id que se va a devolver por chunk, el seq es lo que hace que sea uno por chunk
def _chunk_id(url_final: str, anchor: Optional[str], seq: int) -> str:
    """es el DNI del chunk (único por página+anchor+posición). Lo usás para guardar/actualizar en el store:
    si la sección cambia de contenido mañana, mantiene el mismo ID y el upsert reemplaza la versión vieja 
    (evita duplicados con cambios mínimos de texto). También distingue varios chunks bajo un mismo anchor vía seq."""
    key = f"{url_final}#{anchor or ''}|{seq}".encode("utf-8")
    return hashlib.sha1(key).hexdigest()


def _dedupe_key(text: str, anchor: Optional[str]) -> str:
    """es un filtro durante la corrida para no emitir dos veces el mismo contenido apuntando al mismo anchor 
    (típicamente dentro de la misma página). No sirve para actualizaciones futuras; solo evita clones “instantáneos” en la generación."""
    payload = (text + "|" + (anchor or "")).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


# ------------------------------- presupuesto -----------------------------------

def _fits(next_text: str, current_text: str) -> Tuple[bool, bool]:
    """
    Evalúa si agregar next_text mantiene el chunk dentro de soft/hard.
    Retorna (aceptable, supera_hard).
    - aceptable=True si current+next <= HARD (y si pasa soft no cortamos aún).
    - supera_hard=True si current+next > HARD (indicador para cortar antes).
    """
    # Construimos una vista local del texto si se agregara next_text
    combined = (current_text + ("\n\n" if current_text and next_text else "") + next_text).strip()
    tokens = estimate_tokens(combined)
    if tokens > HARD_TOKENS:
        return (False, True)
    return (True, tokens > SOFT_TOKENS)


# ------------------------------- empaquetado -----------------------------------

def _emit_chunk(
    out: List[Dict[str, Any]], # donde metemos los chunks ya listos
    seen_keys: set, # guarda las claves de _dedupe_key(text, anchor) para no volver a emitir el mismo (texto + anchor) en esta pagina
    *,
    page: Dict[str, Any], # el json de la pagina (HTML) que contiene provenence, sections.
    section_node: Dict[str, Any], # seccion que define el chunk
    parent_chain: List[Dict[str, Any]], # cadena de ancestros
    text: str, # texto final del chunk que ya armamos antes
    has_code: bool, # flag calculado que dice si algun bloque del chunk fue code
    seq: int, # contador local de chunks dentro de la misma pagina
) -> int:
    """Crea y agrega un chunk si no es duplicado. Devuelve el nuevo seq."""
    if not text:
        return seq  # nada que emitir

    # no hace .get porque es obligatorio, si no esta la url_final falla todo
    url_final = page["provenance"]["url_final"]
    title = page["provenance"].get("title")
    fetched_at = page["provenance"].get("fetched_at")

    # Anchor elegido (puede venir del H2 o del H3, o heredar hacia arriba)
    chosen_anchor = _ascend_anchor(section_node, parent_chain)
    url_citable = f"{url_final}#{chosen_anchor}" if chosen_anchor else url_final

    # Dedupe
    key = _dedupe_key(text, chosen_anchor)
    if key in seen_keys:
        return seq
    seen_keys.add(key)

    # Metadatos mínimos
    level = int(section_node.get("level") or 1)
    heading = section_node.get("heading_text")

    chunk = {
        "id": _chunk_id(url_final, chosen_anchor, seq),
        "text": text,
        "url_final": url_final,
        "url_citable": url_citable,
        "title": title,
        "section_level": level,
        "section_heading": heading,
        "section_anchor": chosen_anchor,
        "fetched_at": fetched_at,
        "has_code": bool(has_code),
    }
    out.append(chunk)
    return seq + 1


# ------------------------------- core público ----------------------------------

def make_chunks(parsed_page: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Punto de entrada del chunker v1.
    - Recorre H2 como base; si no hay H2, usa H1/raíz.
    - Incluye H3 hijos mientras haya presupuesto; si no entra, divide por H3.
    - Nunca corta dentro de code; si un bloque code solo excede HARD, lo emite igual (excepción).
    """
    out: List[Dict[str, Any]] = []
    seen_keys: set = set()
    seq = 0  # índice local para IDs estables

    page = parsed_page
    sections = page.get("sections", []) or []

    # 1) Caso con H2 (ruta principal)
    h2_nodes = _iter_h2_nodes(sections)
    if h2_nodes:
        for h2 in h2_nodes:
            parent_chain_h2 = _parent_chain_for_node(h2, sections)  # para fallback de anchor
            # a) Render de los bloques propios del H2
            h2_heading = _render_heading_line(h2)
            h2_body, h2_has_code = _render_blocks(h2.get("blocks", []))
            h2_text = _concat_heading_and_body(h2_heading, h2_body)

            # b) Intentar agregar H3 hijos respetando presupuesto
            current_text = h2_text
            current_has_code = bool(h2_has_code)

            children = h2.get("children", []) or []
            # Si algún H3 no entra, se emite el H2 actual y luego se crean sub-chunks por H3.
            for h3 in children:
                # Render rápido del H3 (enteramente, no se corta en medio)
                h3_heading = _render_heading_line(h3)
                h3_body, h3_has_code = _render_blocks(h3.get("blocks", []))
                h3_text_full = _concat_heading_and_body(h3_heading, h3_body)

                # Si el H3 está vacío, saltar
                if not h3_text_full.strip():
                    continue

                acceptable, exceeds_hard_if_add = _fits(h3_text_full, current_text)

                if acceptable:
                    # Lo agregamos al chunk base H2
                    sep = "\n\n" if current_text and h3_text_full else ""
                    current_text = (current_text + sep + h3_text_full).strip()
                    current_has_code = current_has_code or h3_has_code
                else:
                    # No entra. Emitimos el H2 actual (si tiene algo) y luego sub-chunks por H3
                    if current_text:
                        seq = _emit_chunk(
                            out,
                            seen_keys,
                            page=page,
                            section_node=h2,               # anclado al H2
                            parent_chain=parent_chain_h2,
                            text=current_text,
                            has_code=current_has_code,
                            seq=seq,
                        )
                    # Reiniciamos el acumulador para evitar arrastrar contenido
                    current_text = ""
                    current_has_code = False

                    # Empezamos sub-chunks a nivel H3 desde este y los siguientes
                    seq = _emit_h3_subchunks(
                        out,
                        seen_keys,
                        page=page,
                        h2_parent=h2,
                        h3_list=[h3] + children[children.index(h3) + 1:],
                        start_seq=seq,
                    )
                    # Ya manejamos todos los H3 restantes; cortar el bucle
                    break
            else:
                # Si terminamos el for sin break, emitimos el chunk H2 construido
                if current_text:
                    seq = _emit_chunk(
                        out,
                        seen_keys,
                        page=page,
                        section_node=h2,
                        parent_chain=parent_chain_h2,
                        text=current_text,
                        has_code=current_has_code,
                        seq=seq,
                    )

        return out

    # 2) Fallback: no hay H2 → un solo chunk desde H1/raíz; si excede HARD, partimos por bordes de bloque.
    h1 = _first_h1_or_root(sections)
    parent_chain_h1 = _parent_chain_for_node(h1, sections)
    heading = _render_heading_line(h1)
    body, has_code = _render_blocks(h1.get("blocks", []))
    text = _concat_heading_and_body(heading, body)

    # Intentar incluir también sus hijos (cualquiera que existan) como si fueran “subsecciones”
    for child in h1.get("children", []) or []:
        ch_head = _render_heading_line(child)
        ch_body, ch_has_code = _render_blocks(child.get("blocks", []))
        ch_text = _concat_heading_and_body(ch_head, ch_body)
        if not ch_text:
            continue
        acceptable, exceeds_hard_if_add = _fits(ch_text, text)
        if acceptable:
            sep = "\n\n" if text and ch_text else ""
            text = (text + sep + ch_text).strip()
            has_code = has_code or ch_has_code
        else:
            # Emitir lo acumulado y luego emitir el hijo por separado (como sub-chunk “H3-like”)
            if text:
                seq = _emit_chunk(out, seen_keys, page=page, section_node=h1, parent_chain=parent_chain_h1,
                                  text=text, has_code=has_code, seq=seq)
            # Emitimos el child como chunk propio (anclado al child si tiene anchor)
            seq = _emit_chunk(out, seen_keys, page=page, section_node=child,
                              parent_chain=parent_chain_h1 + [h1], text=ch_text,
                              has_code=ch_has_code, seq=seq)
            # Reiniciar acumulador
            text, has_code = "", False

    if text:
        seq = _emit_chunk(out, seen_keys, page=page, section_node=h1, parent_chain=parent_chain_h1,
                          text=text, has_code=has_code, seq=seq)

    return out


# ----------------------- sub-chunks a nivel H3 (split por H3) -------------------

def _emit_h3_subchunks(
    out: List[Dict[str, Any]],
    seen_keys: set,
    *,
    page: Dict[str, Any],
    h2_parent: Dict[str, Any],
    h3_list: List[Dict[str, Any]],
    start_seq: int,
) -> int:
    """
    Emite sub-chunks por H3 cuando el H2 “no entra”.
    Política v1: 1 chunk por H3 (no agrupamos varios H3 en uno).
    Si un H3 excede HARD por sí solo (p. ej., bloque de código largo), se emite igual.
    """
    seq = start_seq
    for h3 in h3_list:
        parent_chain_h3 = _parent_chain_for_node(h3, [h2_parent]) + [h2_parent]
        h3_heading = _render_heading_line(h3)
        h3_body, h3_has_code = _render_blocks(h3.get("blocks", []))
        h3_text = _concat_heading_and_body(h3_heading, h3_body)

        # Si está vacío, saltar
        if not h3_text.strip():
            continue

        # Regla de atomicidad: no cortamos dentro de code; si excede HARD, se acepta como excepción.
        seq = _emit_chunk(
            out,
            seen_keys,
            page=page,
            section_node=h3,              # anclado al H3
            parent_chain=[h2_parent],
            text=h3_text,
            has_code=h3_has_code,
            seq=seq,
        )
    return seq


# ----------------------------- parent chain util --------------------------------

def _parent_chain_for_node(target: Dict[str, Any], sections: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Construye una cadena de padres simple para el nodo target dentro del árbol de sections.
    Se usa únicamente para fallback de anchor. Si no se encuentra, devuelve [].
    """
    chain: List[Dict[str, Any]] = []

    def dfs(node: Dict[str, Any], parents: List[Dict[str, Any]]) -> bool:
        if node is target:
            chain.extend(parents)
            return True
        for child in node.get("children", []) or []:
            if dfs(child, parents + [node]):
                return True
        return False

    # Buscar en todos los topos
    for top in sections:
        if dfs(top, []):
            break
    return chain

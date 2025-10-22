"""
convierte un html en un documento estructurado (dict/JSON) con provenence {url_final, title, fetched_at} y sections (arbol H1/H2/H3, y dentro de cada 
seccion bloques basicos (paragraph, code, list))

- Recibe HTML crudo (string), url_final y fetched_at.
- Elimina ruido obvio (scripts/estilos/zonas de navegación).
- Construye jerarquía H1→H2→H3.
- Extrae bloques básicos: paragraph, code, list.
- Captura anchor (id) SOLO en headings.
- Devuelve un dict listo para serializar a JSON.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, List, Optional


# ----------------------------
# Utilidades de normalización
# ----------------------------

def _normalize_ws(text: str) -> str:
    """
    Colapsa espacios/saltos redundantes en texto normal (no código) -> en el texto dentro de un html (parrafos, items de listas) suelen venir espacios no separables (&nbsp; ),
    multiples espacios seguidos, tabs y saltos de linea mezclados, eso empeora la legibilidad y complica el retrieval. Esta funcion normaliza eso para dejarlo prolijo
    """

    if not text:
        return ""
    text = text.replace("\xa0", " ")
    # text.split separa por cualquier whitespace (espacios, tabs, salto de linea, etc.), etc y luego todo eso lo junta con espacios, dejando el texto lindo unicamente separado por espacio entre palabras
    return " ".join(text.split())


# ----------------------------
# Estructuras en memoria
# ----------------------------


@dataclass
class Block:
    type: str  # "paragraph" | "code" | "list"
    # paragraph
    text: Optional[str] = None
    # code
    code: Optional[str] = None
    language: Optional[str] = None
    # list
    ordered: Optional[bool] = None
    items: Optional[List[str]] = None


@dataclass
class Section:
    level: int                    # 1 | 2 | 3
    heading_text: str
    anchor: Optional[str] = None  # id del heading, si existe
    # si pones = [] todas las instancias de la clase van a tener por defecto la misma lista, lo cual esta muy mal
    blocks: List[Block] = field(default_factory=list)
    # ACA ES DONDE EL IMPORT DE ANNOTATIONS ME SALVA LA VIDA
    children: List[Section] = field(default_factory=list)


# ----------------------------
# Parser HTML v1 (estado mínimo)
# ----------------------------

class _DocHTMLParser(HTMLParser):
    """
    Parser de HTML mínimo para v1:
    - Ignora scripts, styles y contenedores de navegación comunes.
    - Reconoce h1/h2/h3 (con id como anchor).
    - Reconoce p, ul/ol + li, pre/code (conservando el texto literal del code).
    - Emite secciones y bloques en orden.
    """

    # Tags cuyo contenido ignoramos por completo (ruido)
    _SKIP_TAGS = {
        "script", "style", "noscript",
        "nav", "header", "footer", "aside",
        # contenedores comunes de UI que no son contenido principal:
        "form"
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        # Título del documento (<title>)
        self.page_title: str = ""
        
        # section_stack es el estado de trabajo, representa el camino abierto en jerarquia en este instante (del H1 actual hacia abajo). Sirve para saber quien es el padre
        # de la proxima seccion. Pero sections es la salida final en forma de arbol, aqui se guardan solo las secciones de nivel superior (los H1 y si no hubiera H1, lo que haga de
        # raiz). Cada uno tendra sus children anidados. Es lo que se serializa a JSON. 
        
        # Pila de secciones abiertas (para anidar h1→h2→h3)
        self.section_stack: List[Section] = []

        # Lista raíz de secciones a devolver, tipicamente H1 y abajo sus hijos
        self.sections: List[Section] = []

        # estoy dentro de un parrafo
        self.in_p: bool = False
        # buffer -> donde amontono los textos de un <p>
        self.p_buf: List[str] = []
        
        # estoy dentro de bloque de codigo <pre>
        self.in_pre: bool = False
        # estoy dentro de codigo <code>
        self.in_code: bool = False
        # buffer pero para codigo
        self.code_buf: List[str] = []
        # lenguaje
        self.code_lang: Optional[str] = None  # e.g., class="language-python"

        # estoy dentro de lista con vinetas
        self.in_ul: bool = False
        # estoy dentro de lista numerada
        self.in_ol: bool = False
        # estoy dentro de un item de lista
        self.in_li: bool = False
        # coleccion de items para cada punto de la lista
        self.list_items: List[str] = []
        # buffer pero para listas
        self.li_buf: List[str] = []

        # Control de anidamiento en zonas a ignorar, las que ignoramos las defino en _SKIP_TAGS, al entrar a vineta de esas +1, al salir -1
        self.skip_depth: int = 0

        # Estamos en <head>
        self.in_head: bool = False
        # estamos en <title>
        self.in_title: bool = False

    # ------------
    # Helpers
    # ------------

    
    def _current_section(self) -> Optional[Section]:
        """devuelve la seccion actual"""
        return self.section_stack[-1] if self.section_stack else None

    
    def _append_block(self, block: Block) -> None:
        """ si seccion_stack esta vacia, crea una seccion ficticia (en section y en section_stack), luego agrega el bloque que recibe como parametro a la seccion de section_stack"""
        
        if not self.section_stack:
            # si no tenemos seccion mas importante aun como un H1, la metemos en la pila de secciones y en la lista de secciones (esta es el arbol que se serializa a json para devolver)
            self.section_stack.append(Section(level=1, heading_text=""))
            self.sections.append(self.section_stack[0])
            
        self.section_stack[-1].blocks.append(block)

    def _push_section(self, level: int, heading_text: str, anchor: Optional[str]) -> None:
        """
        Inserta una nueva sección en la jerarquía usando la pila.

        Dado un nivel `level`, el texto del heading y (opcional) su `anchor`:
        1) Desapila mientras el tope tenga nivel >= `level` (busca al padre inmediato,
            que es la primera sección desde el final con nivel < `level`).
        2) Crea la nueva Section y:
            - si hay padre en la pila, la agrega a sus `children`;
            - si la pila quedó vacía, la agrega a la lista raíz `self.sections`.  (porque no tiene papa)
        3) Empuja la nueva sección a la pila (pasa a ser la sección “actual”).
        """
        if level > 3:
            level = 3

        # “Bajar” la pila hasta el padre de este nivel
        while self.section_stack and self.section_stack[-1].level >= level:
            self.section_stack.pop()

        new_sec = Section(level=level, heading_text=heading_text, anchor=anchor)
        if self.section_stack:
            self.section_stack[-1].children.append(new_sec)
        else:
            self.sections.append(new_sec)
        self.section_stack.append(new_sec)

    # ------------
    # Eventos HTML
    # ------------

    def handle_starttag(self, tag: str, attrs: List[tuple]) -> None:
        t = tag.lower()
        # colocas las claves en minuscula para despreocuparte de errores en el llamado
        attr_dict = {k.lower(): v for k, v in attrs}

        # manejo de <head>/<title>
        if t == "head":
            self.in_head = True
        elif t == "title" and self.in_head:
            self.in_title = True

        # zonas a ignorar completas
        if t in self._SKIP_TAGS:
            self.skip_depth += 1
            return
        if self.skip_depth > 0:
            # Si estamos dentro de una zona ignorada, nada más que hacer
            return

        # headings: h1/h2/h3
        if t in ("h1", "h2", "h3"):
            # Antes de abrir un heading, “cerramos” cualquier bloque abierto (p/li/code)
            self._close_open_block_if_any()

            # Guardamos el nivel y almacenamos temporalmente el texto del heading via handle_data
            self._heading_level = {"h1": 1, "h2": 2, "h3": 3}[t]
            self._heading_anchor = attr_dict.get("id")
            self._heading_buf: List[str] = []
            self._in_heading = True
            return

        # párrafo
        if t == "p":
            self.in_p = True
            self.p_buf = []
            return

        # listas
        if t == "ul":
            self.in_ul = True
            self.list_items = []
            return
        if t == "ol":
            self.in_ol = True
            self.list_items = []
            return
        if t == "li":
            self.in_li = True
            self.li_buf = []
            return

        # código: <pre> y/o <code>
        if t == "pre":
            # El bloque de código empieza en <pre>; capturamos literal hasta </pre>
            self.in_pre = True
            self.code_buf = []
            self.code_lang = None
            return
        if t == "code":
            # El lenguaje puede venir en class="language-xxx" o "lang-xxx"
            self.in_code = True
            klass = (attr_dict.get("class") or "") + " " + (attr_dict.get("data-lang") or "")
            lang = None
            for token in klass.split():
                tl = token.lower()
                if tl.startswith("language-"):
                    lang = token.split("-", 1)[1]
                    break
                if tl.startswith("lang-"):
                    lang = token.split("-", 1)[1]
                    break
            if lang:
                self.code_lang = self.code_lang or lang  # solo setear si no había
            return

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()

        # cierre de <head>/<title>
        if t == "title" and self.in_title:
            self.in_title = False
            return
        if t == "head" and self.in_head:
            self.in_head = False
            return

        # salida de zonas ignoradas
        if t in self._SKIP_TAGS:
            if self.skip_depth > 0:
                self.skip_depth -= 1
            return
        if self.skip_depth > 0:
            return

        # cierre de heading
        if t in ("h1", "h2", "h3") and getattr(self, "_in_heading", False):
            text = _normalize_ws("".join(self._heading_buf))
            self._push_section(self._heading_level, text, self._heading_anchor)
            self._in_heading = False
            self._heading_buf = []
            self._heading_anchor = None
            return

        # cierre de párrafo
        if t == "p" and self.in_p:
            self.in_p = False
            text = _normalize_ws("".join(self.p_buf))
            if text:
                self._append_block(Block(type="paragraph", text=text))
            self.p_buf = []
            return

        # cierre de li / ul / ol
        if t == "li" and self.in_li:
            self.in_li = False
            item_text = _normalize_ws("".join(self.li_buf))
            if item_text:
                self.list_items.append(item_text)
            self.li_buf = []
            return

        if t == "ul" and self.in_ul:
            self.in_ul = False
            if self.list_items:
                self._append_block(Block(type="list", ordered=False, items=self.list_items.copy()))
            self.list_items = []
            return

        if t == "ol" and self.in_ol:
            self.in_ol = False
            if self.list_items:
                self._append_block(Block(type="list", ordered=True, items=self.list_items.copy()))
            self.list_items = []
            return

        # cierre de code/pre
        if t == "code" and self.in_code:
            self.in_code = False
            return

        if t == "pre" and self.in_pre:
            self.in_pre = False
            # Para code, preservamos literal tal cual (sin normalizar espacios)
            code_text = "".join(self.code_buf)
            if code_text.strip():  # permitir espacios internos, pero evitar bloques vacíos
                self._append_block(Block(type="code", code=code_text, language=self.code_lang))
            self.code_buf = []
            self.code_lang = None
            return

    def handle_data(self, data: str) -> None:
        if not data:
            return

        # título de página (<title>)
        if self.in_title and self.in_head:
            # No normalizamos aquí; lo haremos al final si hace falta.
            self.page_title += data
            return

        # Si estamos dentro de zona ignorada, descartamos
        if self.skip_depth > 0:
            return

        # heading buffer
        if getattr(self, "_in_heading", False):
            self._heading_buf.append(data)
            return

        # bloques
        if self.in_pre:
            # En <pre>, conservamos literal (incl. saltos y espacios)
            self.code_buf.append(data)
            return

        if self.in_li:
            self.li_buf.append(data)
            return

        if self.in_p:
            self.p_buf.append(data)
            return

        # Texto "sueltos" fuera de <p> y fuera de lista/código:
        # En v1 los ignoramos (muchas veces son piezas de UI). Si quisieras,
        # podrías agregarlos como párrafos huérfanos, pero no es necesario.

    def _close_open_block_if_any(self) -> None:
        """Si había un bloque abierto (p/li/pre), lo cierra emitiendo lo que corresponda."""
        # cerrar <p> abierto
        if self.in_p:
            self.in_p = False
            text = _normalize_ws("".join(self.p_buf))
            if text:
                self._append_block(Block(type="paragraph", text=text))
            self.p_buf = []
        # cerrar <li> abierto
        if self.in_li:
            self.in_li = False
            item_text = _normalize_ws("".join(self.li_buf))
            if item_text:
                self.list_items.append(item_text)
            self.li_buf = []
        # cerrar <ul>/<ol> si estaban abiertos (emitir la lista)
        if self.in_ul:
            self.in_ul = False
            if self.list_items:
                self._append_block(Block(type="list", ordered=False, items=self.list_items.copy()))
            self.list_items = []
        if self.in_ol:
            self.in_ol = False
            if self.list_items:
                self._append_block(Block(type="list", ordered=True, items=self.list_items.copy()))
            self.list_items = []
        # cerrar <pre> (code) abierto
        if self.in_pre:
            self.in_pre = False
            code_text = "".join(self.code_buf)
            if code_text.strip():
                self._append_block(Block(type="code", code=code_text, language=self.code_lang))
            self.code_buf = []
            self.code_lang = None


# ----------------------------
# API principal del módulo
# ----------------------------

def parse_document(*, html_text: str, url_final: str, fetched_at: str) -> Dict:
    """
    Convierte un HTML crudo en un documento estructurado v1.

    Entradas:
      - html_text: HTML como string.
      - url_final: URL definitiva para citas.
      - fetched_at: timestamp ISO de la descarga original.

    Salida (dict):
      {
        "provenance": { "url_final": str, "title": str, "fetched_at": str },
        "sections": [ { "level": int, "heading_text": str, "anchor": str?, "blocks": [...], "children": [...] } ]
      }
    """
    parser = _DocHTMLParser()
    parser.feed(html_text)
    parser.close()

    # Determinar título: preferimos <title>; si está vacío, usamos primer H1
    title = (parser.page_title or "").strip()
    if not title:
        # buscar primer H1
        first_h1 = None
        for s in parser.sections:
            if s.level == 1:
                first_h1 = s.heading_text.strip()
                if first_h1:
                    break
        if first_h1:
            title = first_h1

    # Regla de recuperación: si no hay ninguna sección,
    # creamos una root nivel 1 vacía para no devolver doc vacío.
    if not parser.sections:
        parser.sections.append(Section(level=1, heading_text=title or ""))

    # Si hay una root nivel 1 sin título (porque llegaron bloques antes),
    # y tenemos un título de página, lo rellenamos.
    root0 = parser.sections[0]
    if root0.level == 1 and not root0.heading_text and title:
        root0.heading_text = title

    def section_to_dict(s: Section) -> Dict:
        """Serializar dataclasses a estructuras dict/list nativas"""
        return {
            "level": s.level,
            "heading_text": s.heading_text,
            **({"anchor": s.anchor} if s.anchor else {}),
            "blocks": [block_to_dict(b) for b in s.blocks],
            "children": [section_to_dict(c) for c in s.children],
        }

    def block_to_dict(b: Block) -> Dict:
        if b.type == "paragraph":
            return {"type": "paragraph", "text": b.text or ""}
        if b.type == "code":
            # Importante: código literal, no tocar espacios.
            return {"type": "code", "code": b.code or "", **({"language": b.language} if b.language else {})}
        if b.type == "list":
            return {"type": "list", "ordered": bool(b.ordered), "items": b.items or []}
        # fallback defensivo (no debería pasar en v1)
        return {"type": b.type}

    doc = {
        "provenance": {
            "url_final": url_final,
            "title": title,
            "fetched_at": fetched_at,
        },
        "sections": [section_to_dict(s) for s in parser.sections],
    }

    # Validación mínima v1: al menos una sección y algún contenido posible
    if not doc["sections"]:
        return {}
    return doc


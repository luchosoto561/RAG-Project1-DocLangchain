from __future__ import annotations

import os
import asyncio
from typing import AsyncGenerator, Any, Dict, List

from langchain_core.documents import Document
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
# from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone

# ============================
# 1) CONFIG / CONSTANTES
# ============================

# Prompt "de sistema": acá definís el comportamiento general del asistente.
SYSTEM_PROMPT = """
Sos un asistente de preguntas y respuestas que usa un sistema RAG.

- Respondés SIEMPRE en español, a menos que el usuario pida otro idioma.
- Usás SOLO la información del contexto cuando sea posible.
- Si el contexto no esta relacionado con las respuestas indicalo, pero responde con lo que sabes aclarando que no esta verifiacada la informacion.
""".strip()


# Plantilla para el mensaje del usuario (la parte "humana" del prompt).
# Metemos:
# - un resumen del historial,
# - la pregunta actual,
# - y el contexto RAG recuperado.
USER_PROMPT_TEMPLATE = """
Historial previo (resumen):

{history}

---

Pregunta del usuario:

{question}

---

Contexto relevante recuperado (puede estar en inglés u otro idioma):

{context}

---

Instrucciones:

1. Usá el contexto para responder de forma precisa.
2. si te habla de un tema que no tenes contexto y que no esta relacionado con langchain/langsmith/langgraph/python no ofrescas mas informacion de esos temas, se cortante.
3. Respondé de forma clara y estructurada.

ejemplo de lo que no tenes que hacer:

que es buenos aires? no le digas algo asi -> "Si querés, puedo ayudarte con más detalles sobre Buenos Aires, aunque no están en el contexto actual. ¿Querés?.", es decir, respondele pero no le ofrezcas mas ayuda
""".strip()

from dotenv import load_dotenv

# Carga las variables de entorno desde el archivo .env
load_dotenv()


# ============================
# 2) HELPERS DE FORMATEO
# ============================


def _format_history(messages: List[Dict[str, str]], max_chars: int = 2000) -> str:
    """
    Convierte el historial de mensajes (user/assistant) en un string simple
    para meterlo en el prompt.

    messages: lista de dicts {"role": "user"|"assistant", "content": "..."}
    """
    # Ejemplo simple: role: contenido
    lines: List[str] = []
    for msg in messages:
        role = msg.get("role", "user")
        if role == "user":
            prefix = "Usuario"
        elif role == "assistant":
            prefix = "Asistente"
        else:
            prefix = role

        content = msg.get("content", "").strip()
        if not content:
            continue

        lines.append(f"{prefix}: {content}")

    history_str = "\n".join(lines)

    # Si el historial es muy largo, lo recortamos brutalmente.
    # Más adelante podrías reemplazar esto por un "summarizer" con LLM.
    if len(history_str) > max_chars:
        history_str = history_str[-max_chars:]

    return history_str


def _format_docs(docs: List[Document]) -> str:
    """
    Convierte los Document de LangChain en un solo string,
    con numeración [1], [2], etc., para que el modelo pueda referenciar fuentes.
    """
    formatted_chunks: List[str] = []
    for i, doc in enumerate(docs, start=1):
        # Contenido principal del chunk.
        content = (doc.page_content or "").strip()

        # Metadata opcional (source, página, etc.)
        meta = doc.metadata or {}
        source = meta.get("source") or meta.get("url") or meta.get("file_name")
        extra = []
        if source:
            extra.append(f"source={source}")
        if "page" in meta:
            extra.append(f"page={meta['page']}")

        meta_str = f" ({', '.join(extra)})" if extra else ""

        formatted_chunks.append(f"[{i}]{meta_str}\n{content}")

    if not formatted_chunks:
        return "No se recuperó contexto relevante."

    return "\n\n".join(formatted_chunks)


def build_citations_from_docs(docs: List[Document]) -> List[Dict[str, str]]:
    """
    Construye la lista de citas a partir de los Document recuperados.

    Cada cita tiene la forma:
        {"title": "<titulo>", "url": "<url>"}

    - Usa 'title' desde doc.metadata["title"] si existe.
    - Usa como URL preferente 'url_citable'; si no existe, usa 'url_final'.
    - Elimina duplicados por URL (si varios chunks vienen de la misma página).
    """
    citations: List[Dict[str, str]] = []
    seen_urls: set[str] = set()

    for doc in docs:
        metadata = doc.metadata or {}

        title = metadata.get("title")
        # prioridad: url_citable, si no, url_final
        url = metadata.get("url_citable") or metadata.get("url_final")

        # si no tenemos URL, no tiene mucho sentido como cita clickeable
        if not url:
            continue

        # evitamos duplicar la misma URL varias veces
        if url in seen_urls:
            continue

        seen_urls.add(url)

        # si no hay título, usamos algo genérico
        if not title:
            title = "Fuente"

        citations.append(
            {
                "title": title,
                "url": url,
            }
        )

    return citations

# ============================
# 3) RETRIEVER (VECTOR DB)
# ============================

# Embeddings para Pinecone (usa OPENAI_API_KEY desde el entorno)
_embeddings = OpenAIEmbeddings(
    model="text-embedding-3-small",
)

# Nombre del índice y namespace
_PINECONE_INDEX_NAME = os.environ["PINECONE_INDEX"]
_PINECONE_NAMESPACE = "dev"          # el namespace donde cargaste los 650 records
_TEXT_FIELD_NAME = "text"            # campo de metadata donde guardaste el chunk

# Cliente e índice de Pinecone (serverless)
_pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
_index = _pc.Index(_PINECONE_INDEX_NAME)

async def retrieve_relevant_docs(question: str, k: int = 4) -> List[Document]:
    
    query_vector = await _embeddings.aembed_query(question)

    res = _index.query(
        namespace=_PINECONE_NAMESPACE,
        vector=query_vector,
        top_k=k,
        include_metadata=True,
    )

    matches = getattr(res, "matches", None)
    if matches is None:
        matches = res.get("matches", [])

    docs: List[Document] = []

    for m in matches:
        metadata = getattr(m, "metadata", None)
        if metadata is None and isinstance(m, dict):
            metadata = m.get("metadata", {}) or {}
        if metadata is None:
            metadata = {}

        text = (metadata.get(_TEXT_FIELD_NAME) or "").strip()

        docs.append(
            Document(
                page_content=text,
                metadata=metadata,
            )
        )
        print(f"el contenido del chunk recuperado es: {text}")

    print(f"[Pinecone] matches dev: {len(docs)}", flush=True)
    return docs

# ============================
# 4) SETUP DEL LLM + CHAIN
# ============================


def _build_llm() -> ChatOpenAI:
    """
    Crea la instancia del modelo de chat.

    - Podés cambiar el modelo, temperatura, etc.
    - LangSmith se integra automáticamente si seteás las env vars:
      LANGCHAIN_TRACING_V2=true y LANGCHAIN_API_KEY=...
    """
    return ChatOpenAI(
        model="gpt-4.1-mini",  # Cambiá esto al modelo que uses
        temperature=0.2,
        streaming=True,  # IMPORTANTE para poder hacer astream
    )


def _build_chain():
    """
    Construye la chain (prompt -> LLM -> parser).

    Usamos:
      - ChatPromptTemplate
      - ChatOpenAI (streaming)
      - StrOutputParser (para que nos devuelva strings)
    """
    llm = _build_llm()

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", SYSTEM_PROMPT),
            ("human", USER_PROMPT_TEMPLATE),
        ]
    )

    # La chain recibe un dict con:
    # - history: str
    # - question: str
    # - context: str
    #
    # y devuelve tokens de texto en streaming.
    chain = prompt | llm | StrOutputParser()

    return chain


# ============================
# 5) FUNCIÓN PRINCIPAL: respond_stream
# ============================


async def respond_stream(messages: List[Any]) -> AsyncGenerator[Dict[str, Any], None]:
    
    # -------------------------
    # 1) Normalizar mensajes
    # -------------------------
    
    normalized: List[Dict[str, str]] = []
    for m in messages:
        # m puede ser un BaseModel (pydantic) o ya un dict
        role = getattr(m, "role", None) or m.get("role")
        content = getattr(m, "content", None) or m.get("content")
        if not role or not content:
            continue
        normalized.append({"role": role, "content": content})

    if not normalized:
        yield {
            "type": "error",
            "message": "No se recibieron mensajes válidos.",
        }
        return

    last_message = normalized[-1]
    question = last_message["content"].strip()

    history_messages = normalized[:-1]
    history_str = _format_history(history_messages)

    # -------------------------
    # 2) Recuperar contexto (RAG)
    # -------------------------
    try:
        docs = await retrieve_relevant_docs(question, k=4)
        # lista de citas que luego se devolverá en el último evento SSE
        citations: List[Dict[str, str]] = build_citations_from_docs(docs)
    
    except Exception as e:
        # Si falla el retriever, avisamos al frontend y abortamos.
        yield {
            "type": "error",
            "message": "Error al recuperar el contexto desde la base vectorial.",
            "detail": str(e),  # en producción lo podés omitir
        }
        return

    # Construimos el contexto para el prompt.
    context_str = _format_docs(docs)

    # -------------------------
    # 3) Construir chain (prompt + llm)
    # -------------------------
    chain = _build_chain()

    # Input que va a consumir la chain (coincide con las variables del prompt).
    chain_input = {
        "history": history_str,
        "question": question,
        "context": context_str,
    }

    # -------------------------
    # 4) Llamar al LLM en streaming
    # -------------------------

    full_answer = ""

    try:
        async for chunk in chain.astream(chain_input):
            if not chunk:
                continue

            full_answer += chunk

            yield {
                "chunk": chunk,
            }

            await asyncio.sleep(0)  

    except asyncio.CancelledError:
        return
    except Exception as e:
        yield {
            "type": "error",
            "message": "Ocurrió un error al generar la respuesta.",
            "detail": str(e),  # lo podés borrar en producción
        }
        return

    # -------------------------
    # 5) Evento final
    # -------------------------
    # En este punto ya terminamos de streamear todos los tokens.
    # Mandamos un evento final con:
    # - respuesta completa
    # - fuentes (simplificadas)
    yield {
        "done": True,
        "citations":citations
    }

    
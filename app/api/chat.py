from __future__ import annotations

import asyncio
import json
from typing import Literal, List

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

# Punto único de entrada al RAG (lo implementás en app/rag.py)
from app.rag import respond_stream  # debe ser un async generator que emite dicts

router = APIRouter()


# ===== Schemas mínimos del request =====
Role = Literal["user", "assistant"]


class Message(BaseModel):
    role: Role
    content: str = Field(min_length=1)


class ChatRequest(BaseModel):
    messages: List[Message] = Field(min_length=1)


# ===== Helper SSE =====
def sse(event: dict) -> str:
    """Formatea un evento SSE: 'data: {...}\\n\\n'"""
    return f"data: {json.dumps(event, ensure_ascii=False)}\n\n"


# aca la magia la hace el StreamingResponse que lo explico en mi notion al detalle
@router.post("/chat")
async def chat(payload: ChatRequest):
    async def event_source():
        try:
            async for event in respond_stream(payload.messages):
                yield sse(event)
        except asyncio.CancelledError:
            return

    return StreamingResponse(
        event_source(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache"},
    )
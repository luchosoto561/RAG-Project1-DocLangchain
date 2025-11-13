# app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.chat import router as chat_router  # lo implementamos después

app = FastAPI(title="RAG LangChain API", version="0.1.0")

# CORS (dev): permite llamadas desde tu front en localhost:3000
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)

@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"status": "ok"}

@app.get("/version", include_in_schema=False)
def version():
    return {"version": app.version}

# Rutas de la API
app.include_router(chat_router)
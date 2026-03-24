"""
FastAPI backend for the O2C Graph Query System.

Routes:
  GET  /                      → serves frontend/index.html
  POST /api/load-data         → triggers ingest from data/ directory
  GET  /api/graph             → full graph (cytoscape elements)
  GET  /api/graph/node/{id}   → node neighbors (expand on click)
  POST /api/query             → NL query → SQL → answer + highlights
  GET  /api/schema            → table list + columns
  GET  /api/status            → health check
"""

import os
import json
import asyncio
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from database import get_connection
from ingest import ingest_all
from graph_builder import build_graph, graph_to_cytoscape, get_node_neighbors, get_graph, detect_anomalies, semantic_search
from llm_service import handle_query
from schema import get_table_list, get_table_columns

app = FastAPI(title="O2C Graph Query System", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

# Serve static files
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


# ── Models ────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    question: str
    conversation_history: list[dict] | None = None


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
async def serve_frontend():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"message": "O2C Graph API is running. Frontend not found."}


@app.post("/api/load-data")
async def load_data():
    """Ingest all JSONL files from data/ into SQLite."""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, ingest_all)
        # Rebuild graph after ingest
        await loop.run_in_executor(None, build_graph)
        tables = get_table_list()
        return {"status": "ok", "tables_loaded": tables}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/graph")
async def get_full_graph(max_nodes: int = 300):
    """Return full graph as Cytoscape.js elements."""
    try:
        G = get_graph()
        data = graph_to_cytoscape(G, max_nodes=max_nodes)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/graph/node/{node_id:path}")
async def get_node_context(node_id: str, depth: int = 1):
    """Return immediate neighborhood of a node for expand-on-click."""
    try:
        data = get_node_neighbors(node_id, depth=depth)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/query")
async def query(req: QueryRequest):
    """
    Natural language query → SQL → execute → NL answer + highlighted node IDs.
    """
    if not req.question or len(req.question.strip()) < 3:
        raise HTTPException(status_code=400, detail="Question too short")

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        None,
        lambda: handle_query(req.question, req.conversation_history),
    )
    return result


@app.get("/api/schema")
async def get_schema():
    """Return all table names and their columns."""
    tables = get_table_list()
    schema = {}
    for t in tables:
        schema[t] = get_table_columns(t)
    return {"tables": schema}


@app.get("/api/status")
async def status():
    conn = get_connection()
    tables = get_table_list()
    counts = {}
    for t in tables:
        try:
            row = conn.execute(f'SELECT COUNT(*) as n FROM "{t}"').fetchone()
            counts[t] = row["n"]
        except Exception:
            counts[t] = 0
    conn.close()
    G = get_graph()
    return {
        "status": "ok",
        "db_tables": len(tables),
        "row_counts": counts,
        "graph_nodes": G.number_of_nodes(),
        "graph_edges": G.number_of_edges(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)


# ── New Feature Routes ────────────────────────────────────────────────────────

class SearchRequest(BaseModel):
    query: str
    max_results: int = 50

@app.post("/api/search")
async def semantic_search_route(req: SearchRequest):
    """Semantic search over node attributes → returns matching node IDs."""
    if not req.query or len(req.query.strip()) < 2:
        raise HTTPException(status_code=400, detail="Query too short")
    loop = asyncio.get_event_loop()
    node_ids = await loop.run_in_executor(None, lambda: semantic_search(req.query, req.max_results))
    return {"node_ids": node_ids, "count": len(node_ids), "query": req.query}


@app.get("/api/anomalies")
async def get_anomalies():
    """Detect broken O2C flows and return anomaly groups."""
    loop = asyncio.get_event_loop()
    anomalies = await loop.run_in_executor(None, detect_anomalies)
    return {"anomalies": anomalies}
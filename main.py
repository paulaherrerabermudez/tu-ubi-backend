from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pathlib import Path
from supabase_client import select_from_view, public_image_url


app = FastAPI(title="TU UBI SIG Backend", version="0.1")

@app.get("/api/listings")
def get_listings(limit: int = 20):
    rows = select_from_view("listings_feed", "*", limit)

    for r in rows:
        r["cover_url"] = public_image_url(r.get("cover_path"))

    return {"items": rows}


# CORS para que Lovable (y tu web) puedan consumir la API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # en producción lo restringimos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "tabla_manzanas.csv"

def load_table() -> pd.DataFrame:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"No existe {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, dtype={"MANCODIGO": str})
    return df

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/manzanas")
def manzanas(
    limit: int = Query(200, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    min_sitp: int | None = None,
    min_parques: int | None = None,
):
    """
    Devuelve filas de la tabla (con filtros opcionales).
    """
    df = load_table()

    if min_sitp is not None and "SITP_Count" in df.columns:
        df = df[df["SITP_Count"] >= min_sitp]
    if min_parques is not None and "PARQUES_Count" in df.columns:
        df = df[df["PARQUES_Count"] >= min_parques]

    total = int(len(df))
    page = df.iloc[offset : offset + limit].to_dict(orient="records")
    return {"total": total, "limit": limit, "offset": offset, "data": page}

@app.get("/top")
def top(
    n: int = Query(10, ge=1, le=100),
    w_sitp: float = 1.0,
    w_parques: float = 1.0,
):
    """
    Top N manzanas por score ponderado.
    Usa *_Score (1-5). Si una columna no existe, la trata como 0.
    """
    df = load_table()

    # Asegurar columnas
    for col in ["SITP_Score", "PARQUES_Score"]:
        if col not in df.columns:
            df[col] = 0

    df["TOTAL_Score"] = (df["SITP_Score"] * w_sitp) + (df["PARQUES_Score"] * w_parques)

    out = (
        df.sort_values("TOTAL_Score", ascending=False)
          .head(n)[["MANCODIGO", "SITP_Count", "SITP_Score", "PARQUES_Count", "PARQUES_Score", "TOTAL_Score"]]
          .to_dict(orient="records")
    )
    return {"n": n, "weights": {"sitp": w_sitp, "parques": w_parques}, "data": out}

from typing import Dict, Any

@app.post("/search")
def search(payload: Dict[str, Any]):
    return {
        "results": []
    }


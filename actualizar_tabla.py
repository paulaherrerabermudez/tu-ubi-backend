import os
import math
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

# =======================
# CONFIG
# =======================
MANZANAS_GPKG = "manzanas.gpkg"
MANZANAS_LAYER = "manzanas"
TABLA_CSV = "tabla_manzanas.csv"

# URLs REST
SITP_URL = "https://serviciosgis.catastrobogota.gov.co/arcgis/rest/services/movilidad/transportepublico/MapServer/5"
PARQUES_URL = "https://serviciosgis.catastrobogota.gov.co/arcgis/rest/services/recreaciondeporte/parquesyescenarios/MapServer/1"

BUFFER_METROS = 800
BATCH = 2000

# =======================
# HELPERS
# =======================
def esri_to_geojson(geom):
    if geom is None:
        return None
    if "x" in geom and "y" in geom:
        return {"type": "Point", "coordinates": [geom["x"], geom["y"]]}
    if "paths" in geom:
        return {"type": "MultiLineString", "coordinates": geom["paths"]}
    if "rings" in geom:
        return {"type": "Polygon", "coordinates": geom["rings"]}
    return None


def fetch_all_features(layer_url, batch=2000):
    query_url = layer_url.rstrip("/") + "/query"

    # total features
    total = requests.get(
        query_url,
        params={
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "json",
            "returnCountOnly": "true",
        },
        timeout=60,
    ).json()["count"]

    features = []
    for offset in range(0, total, batch):
        r = requests.get(
            query_url,
            params={
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "true",
                "f": "json",
                "resultOffset": offset,
                "resultRecordCount": batch,
                "outSR": 4326,
            },
            timeout=120,
        )
        r.raise_for_status()
        features.extend(r.json().get("features", []))
        print(f"Descargados {len(features)}/{total}")

    return features


def features_to_gdf(features):
    rows, geoms = [], []
    for f in features:
        rows.append(f.get("attributes", {}))
        gj = esri_to_geojson(f.get("geometry"))
        geoms.append(shape(gj) if gj else None)

    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")
    return gdf[~gdf.geometry.isna()].copy()


def quintile_score(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0)

    if s.nunique() <= 1:
        return pd.Series([1] * len(s), index=s.index)

    return (
        s.rank(pct=True, method="average")
        .mul(5)
        .apply(lambda x: int(math.ceil(x)))
        .clip(1, 5)
    )


# =======================
# MAIN
# =======================
def main():
    # 1) Cargar manzanas
    manz = gpd.read_file(MANZANAS_GPKG, layer=MANZANAS_LAYER).to_crs(epsg=3116)

    if "MANCODIGO" not in manz.columns:
        raise ValueError("La capa manzanas debe tener la columna MANCODIGO")

    base = manz[["MANCODIGO"]].copy()
    base["MANCODIGO"] = base["MANCODIGO"].astype(str)

    # 2) Crear / cargar tabla maestra
    if os.path.exists(TABLA_CSV):
        tabla = pd.read_csv(TABLA_CSV, dtype={"MANCODIGO": str})
        tabla = base.merge(tabla, on="MANCODIGO", how="left")
    else:
        tabla = base.copy()

    # 3) Crear buffer (UNA sola vez)
    buffer = manz[["MANCODIGO", "geometry"]].copy()
    buffer["geometry"] = buffer.geometry.buffer(BUFFER_METROS)

    # =======================
    # CONTEO SITP
    # =======================
    sitp = features_to_gdf(fetch_all_features(SITP_URL, BATCH)).to_crs(epsg=3116)

    joined_s = gpd.sjoin(buffer, sitp[["geometry"]], how="left", predicate="intersects")
    sitp_counts = (
        joined_s.groupby("MANCODIGO")
        .size()
        .rename("SITP_Count")
        .reset_index()
    )

    tabla = tabla.drop(columns=[c for c in ["SITP_Count", "SITP_Score"] if c in tabla.columns])
    tabla = tabla.merge(sitp_counts, on="MANCODIGO", how="left")
    tabla["SITP_Count"] = tabla["SITP_Count"].fillna(0).astype(int)
    tabla["SITP_Score"] = quintile_score(tabla["SITP_Count"]).astype(int)

    # =======================
    # CONTEO PARQUES
    # =======================
    parques = features_to_gdf(fetch_all_features(PARQUES_URL, BATCH)).to_crs(epsg=3116)

    joined_p = gpd.sjoin(buffer, parques[["geometry"]], how="left", predicate="intersects")
    parques_counts = (
        joined_p.groupby("MANCODIGO")
        .size()
        .rename("PARQUES_Count")
        .reset_index()
    )

    tabla = tabla.drop(columns=[c for c in ["PARQUES_Count", "PARQUES_Score"] if c in tabla.columns])
    tabla = tabla.merge(parques_counts, on="MANCODIGO", how="left")
    tabla["PARQUES_Count"] = tabla["PARQUES_Count"].fillna(0).astype(int)
    tabla["PARQUES_Score"] = quintile_score(tabla["PARQUES_Count"]).astype(int)

    # 4) Guardar tabla final
    tabla.to_csv(TABLA_CSV, index=False, encoding="utf-8")

    print("✅ Tabla actualizada correctamente")
    print(tabla[["MANCODIGO", "SITP_Count", "SITP_Score", "PARQUES_Count", "PARQUES_Score"]].head(10))


if __name__ == "__main__":
    main()

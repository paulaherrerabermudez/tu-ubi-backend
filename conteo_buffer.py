import requests
import geopandas as gpd
from shapely.geometry import shape

# ---------- CONFIG ----------
MANZANAS_FILE = "manzanas.gpkg"
MANZANAS_LAYER = "manzanas"

# Paraderos SITP (Catastro Bogotá)
CAPA_URL = "https://serviciosgis.catastrobogota.gov.co/arcgis/rest/services/movilidad/transportepublico/MapServer/5"

# Nombre de la columna de salida
COUNT_FIELD_NAME = "SITP_Count"

# Radio del buffer en metros
BUFFER_METROS = 800

# Paginación de ArcGIS REST
BATCH = 2000

# Campos a solicitar a la capa (para conteo puro, puede ser "*")
OUT_FIELDS = "*"
# ---------------------------


def esri_to_geojson(geom: dict):
    """
    Convierte ESRI JSON (ArcGIS REST) a GeoJSON básico para shapely.shape().
    Soporta Point, Polyline y Polygon.
    """
    if geom is None:
        return None

    # Point
    if "x" in geom and "y" in geom:
        return {"type": "Point", "coordinates": [geom["x"], geom["y"]]}

    # Polyline
    if "paths" in geom:
        return {"type": "MultiLineString", "coordinates": geom["paths"]}

    # Polygon
    if "rings" in geom:
        return {"type": "Polygon", "coordinates": geom["rings"]}

    return None


def fetch_all_features(layer_url: str, where="1=1", out_fields="*", batch=2000):
    """
    Descarga TODAS las features de un ArcGIS REST layer (MapServer/0 o FeatureServer/0)
    usando paginación con resultOffset/resultRecordCount.
    """
    query_url = layer_url.rstrip("/") + "/query"

    # 1) Conteo total
    params_count = {
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "true",
        "f": "json",
        "returnCountOnly": "true",
    }
    r = requests.get(query_url, params=params_count, timeout=60)
    r.raise_for_status()
    total = r.json().get("count", 0)
    print(f"Total features a descargar: {total}")

    # 2) Descarga por bloques
    features = []
    for offset in range(0, total, batch):
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": batch,
            "outSR": 4326,  # fuerza geometría en WGS84
        }
        r = requests.get(query_url, params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        chunk = data.get("features", [])
        features.extend(chunk)
        print(f"Descargadas {len(features)}/{total}")

    return features


def arcgis_features_to_gdf(features, crs="EPSG:4326"):
    """
    Convierte lista de features ArcGIS REST (ESRI JSON) a GeoDataFrame.
    """
    rows = []
    geoms = []
    for f in features:
        rows.append(f.get("attributes", {}))
        gj = esri_to_geojson(f.get("geometry"))
        geoms.append(shape(gj) if gj else None)

    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs=crs)
    gdf = gdf[~gdf.geometry.isna()].copy()
    return gdf


def main():
    # 1) Cargar manzanas
    manz = gpd.read_file(MANZANAS_FILE, layer=MANZANAS_LAYER)
    if manz.crs is None:
        raise ValueError("manzanas.gpkg no tiene CRS definido")
    manz = manz.to_crs(epsg=3116)  # metros

    # Verifica que exista MANCODIGO
    if "MANCODIGO" not in manz.columns:
        raise ValueError("La capa de manzanas no tiene la columna 'MANCODIGO'")

    print(f"Manzanas cargadas: {len(manz)}")

    # 2) Crear buffers
    manz_buf = manz[["MANCODIGO", "geometry"]].copy()
    manz_buf["geometry"] = manz_buf.geometry.buffer(BUFFER_METROS)
    print("Buffer creado (800m).")

    # 3) Descargar capa SITP
    feats = fetch_all_features(CAPA_URL, out_fields=OUT_FIELDS, batch=BATCH)
    capa = arcgis_features_to_gdf(feats, crs="EPSG:4326").to_crs(epsg=3116)
    print(f"Features SITP descargadas: {len(capa)}")

    # 4) Join espacial + conteo
    joined = gpd.sjoin(
        manz_buf,
        capa[["geometry"]],
        how="left",
        predicate="intersects"
    )

    counts = joined.groupby("MANCODIGO").size().reset_index(name=COUNT_FIELD_NAME)

    # 5) Unir conteos a manzanas originales
    out = manz.merge(counts, on="MANCODIGO", how="left")
    out[COUNT_FIELD_NAME] = out[COUNT_FIELD_NAME].fillna(0).astype(int)

    # 6) Guardar resultados
    out.to_file("manzanas_conteo.gpkg", layer="manzanas", driver="GPKG")
    out.drop(columns="geometry").to_csv("manzanas_conteo.csv", index=False, encoding="utf-8")

    print("✅ Listo:")
    print(" - manzanas_conteo.gpkg")
    print(" - manzanas_conteo.csv")


if __name__ == "__main__":
    main()

import requests
import geopandas as gpd
from shapely.geometry import shape

# -------- CONFIG --------
MANZANAS_GPKG = "manzanas.gpkg"
MANZANAS_LAYER = "manzanas"

OUTPUT_GPKG = "buffer_sitp_800m.gpkg"
OUTPUT_LAYER = "buffer_800m_sitp"

SITP_URL = "https://serviciosgis.catastrobogota.gov.co/arcgis/rest/services/movilidad/transportepublico/MapServer/5"

BUFFER_METROS = 800
BATCH = 2000
# ------------------------


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

    # Conteo total
    params_count = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json",
        "returnCountOnly": "true",
    }
    total = requests.get(query_url, params=params_count, timeout=60).json()["count"]

    features = []
    for offset in range(0, total, batch):
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": batch,
            "outSR": 4326,
        }
        r = requests.get(query_url, params=params, timeout=120)
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


def main():
    # 1) Cargar manzanas
    manz = gpd.read_file(MANZANAS_GPKG, layer=MANZANAS_LAYER).to_crs(epsg=3116)

    # 2) Crear buffer 800 m
    buffer = manz[["MANCODIGO", "geometry"]].copy()
    buffer["geometry"] = buffer.geometry.buffer(BUFFER_METROS)

    # 3) Descargar SITP
    sitp_feats = fetch_all_features(SITP_URL, BATCH)
    sitp = features_to_gdf(sitp_feats).to_crs(epsg=3116)

    # 4) Conteo espacial (puntos dentro del buffer)
    joined = gpd.sjoin(
        buffer,
        sitp[["geometry"]],
        how="left",
        predicate="intersects"
    )

    counts = joined.groupby("MANCODIGO").size().reset_index(name="SITP_Count")

    # 5) Unir conteo al buffer
    buffer_final = buffer.merge(counts, on="MANCODIGO", how="left")
    buffer_final["SITP_Count"] = buffer_final["SITP_Count"].fillna(0).astype(int)

    # 6) Guardar capa final
    buffer_final.to_file(
        OUTPUT_GPKG,
        layer=OUTPUT_LAYER,
        driver="GPKG"
    )

    print("✅ Conteo listo")
    print(f"- Archivo: {OUTPUT_GPKG}")
    print(f"- Capa: {OUTPUT_LAYER}")


if __name__ == "__main__":
    main()

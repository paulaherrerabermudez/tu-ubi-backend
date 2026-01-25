import requests
import geopandas as gpd
from shapely.geometry import shape

LAYER_URL = "https://serviciosgis.catastrobogota.gov.co/arcgis/rest/services/catastro/manzana/MapServer/0/query"

def esri_to_geojson(geom: dict) -> dict | None:
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

def fetch_all(out_fields="MANCODIGO", where="1=1", batch=2000):
    # total count
    params = {
        "where": where,
        "outFields": out_fields,
        "returnGeometry": "true",
        "f": "json",
        "returnCountOnly": "true"
    }
    total = requests.get(LAYER_URL, params=params, timeout=60).json()["count"]

    feats = []
    for offset in range(0, total, batch):
        params = {
            "where": where,
            "outFields": out_fields,
            "returnGeometry": "true",
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": batch,
            "outSR": 4326  # fuerza salida en WGS84
        }
        r = requests.get(LAYER_URL, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        chunk = data.get("features", [])
        feats.extend(chunk)
        print(f"Descargadas {len(feats)}/{total}")
    return feats

def main():
    features = fetch_all(out_fields="MANCODIGO")
    rows, geoms = [], []

    for f in features:
        rows.append(f.get("attributes", {}))
        gj = esri_to_geojson(f.get("geometry"))
        geoms.append(shape(gj) if gj else None)

    gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs="EPSG:4326")
    gdf = gdf[~gdf.geometry.isna()].copy()

    # a metros para buffers
    gdf = gdf.to_crs(epsg=3116)

    gdf.to_file("manzanas.gpkg", layer="manzanas", driver="GPKG")
    print("✅ Guardado: manzanas.gpkg (layer=manzanas)")

if __name__ == "__main__":
    main()

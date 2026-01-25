import geopandas as gpd

# -------- CONFIG --------
INPUT_GPKG = "manzanas.gpkg"
INPUT_LAYER = "manzanas"
OUTPUT_GPKG = "manzanas_buffer_800m.gpkg"
OUTPUT_LAYER = "buffer_800m"
BUFFER_METROS = 800
# ------------------------

def main():
    # Cargar manzanas
    gdf = gpd.read_file(INPUT_GPKG, layer=INPUT_LAYER)

    # Asegurar CRS en metros
    if gdf.crs is None:
        raise ValueError("La capa no tiene CRS")
    gdf = gdf.to_crs(epsg=3116)

    # Crear buffer
    gdf_buffer = gdf.copy()
    gdf_buffer["geometry"] = gdf_buffer.geometry.buffer(BUFFER_METROS)

    # Guardar capa de buffer
    gdf_buffer.to_file(
        OUTPUT_GPKG,
        layer=OUTPUT_LAYER,
        driver="GPKG"
    )

    print("✅ Buffer creado:")
    print(f"- Archivo: {OUTPUT_GPKG}")
    print(f"- Capa: {OUTPUT_LAYER}")

if __name__ == "__main__":
    main()

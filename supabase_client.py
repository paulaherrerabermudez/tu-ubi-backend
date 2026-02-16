import os
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # aquí pega tu sb_secret_...
SUPABASE_BUCKET = os.environ.get("SUPABASE_BUCKET", "listing-images")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

def public_image_url(path: str | None) -> str | None:
    if not path:
        return None
    return f"{SUPABASE_URL}/storage/v1/object/public/{SUPABASE_BUCKET}/{path}"

def select_from_view(view_name: str, select: str = "*", limit: int = 20):
    """
    Llama al REST endpoint de PostgREST para leer una VIEW (por ejemplo listings_feed).
    """
    url = f"{SUPABASE_URL}/rest/v1/{view_name}"
    params = {"select": select, "limit": str(limit)}
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


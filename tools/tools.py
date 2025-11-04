# tools.py
"""
Shared reusable tools for mobile_ads_analytics ADK.
- Firestore reads
- BigQuery uploads and query
- Geocoding (Google Maps)
- Helpers: flatten, timestamp conversion, save/load JSON
"""

import os
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from google.cloud import firestore, bigquery

# Optional: requests for geocoding
import requests

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

# Config from env
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "mobile-ads-position-and-time")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", FIREBASE_PROJECT_ID)
BQ_DATASET = os.getenv("BQ_DATASET", "firestore_export")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")  # optional


# -------------------------
# Firestore helpers
# -------------------------
def get_firestore_client() -> firestore.Client:
    return firestore.Client(project=FIREBASE_PROJECT_ID)


def list_collections_sample(sample_size: int = 3) -> Dict[str, Any]:
    db = get_firestore_client()
    out: Dict[str, Any] = {}
    for coll in db.collections():
        name = coll.id
        docs = []
        for d in coll.limit(sample_size).stream():
            docs.append({**d.to_dict(), "__id": d.id})
        out[name] = {"sample_count": len(docs), "sample": docs}
    logging.info("list_collections_sample: found %d collections", len(out))
    return out


def read_firestore_subset(
    collection: str,
    filters: Optional[Dict[str, Any]] = None,
    limit: int = 100,
    order_by: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Read subset of documents from a Firestore collection.
    filters: dict of field -> value (equality conditions)
    """
    db = get_firestore_client()
    ref = db.collection(collection)
    if filters:
        for k, v in filters.items():
            ref = ref.where(k, "==", v)
    if order_by:
        ref = ref.order_by(order_by, direction=firestore.Query.DESCENDING)
    docs = list(ref.limit(limit).stream())
    rows = [{**d.to_dict(), "Id": d.id} for d in docs]
    logging.info("read_firestore_subset: %d rows from %s", len(rows), collection)
    return rows


# -------------------------
# BigQuery helpers
# -------------------------
def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=BQ_PROJECT_ID)


def ensure_dataset(dataset: str) -> None:
    client = get_bq_client()
    dataset_id = f"{BQ_PROJECT_ID}.{dataset}"
    try:
        client.get_dataset(dataset_id)
        logging.debug("Dataset exists: %s", dataset_id)
    except Exception:
        ds = bigquery.Dataset(dataset_id)
        ds.location = "US"
        client.create_dataset(ds)
        logging.info("Created dataset: %s", dataset_id)


def upload_rows_to_bq(
    dataset: str,
    table: str,
    rows: List[Dict[str, Any]],
    write_disposition: str = "WRITE_APPEND",
    autodetect: bool = True,
) -> str:
    """
    Upload rows (list of dict) to BigQuery. Creates dataset if missing.
    Returns full table id string: project.dataset.table
    """
    if not rows:
        raise ValueError("No rows provided to upload_rows_to_bq")

    ensure_dataset(dataset)
    client = get_bq_client()
    table_id = f"{BQ_PROJECT_ID}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        autodetect=autodetect,
        write_disposition=getattr(bigquery.WriteDisposition, write_disposition),
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # BigQuery client supports load_table_from_json
    job = client.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(autodetect=autodetect, write_disposition=bigquery.WriteDisposition.WRITE_APPEND))
    job.result()  # wait
    logging.info("upload_rows_to_bq: uploaded %d rows to %s", len(rows), table_id)
    return table_id


def run_query(sql: str) -> List[Dict[str, Any]]:
    client = get_bq_client()
    job = client.query(sql)
    rows = [dict(r) for r in job.result()]
    logging.info("run_query: returned %d rows", len(rows))
    return rows


# -------------------------
# Geocoding / Maps helpers
# -------------------------
def geocode_place(place_name: str) -> Optional[Dict[str, float]]:
    """Return {'lat':..., 'lng':...} or None."""
    key = GOOGLE_MAPS_API_KEY
    if not key:
        logging.warning("geocode_place: GOOGLE_MAPS_API_KEY not set")
        return None
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    resp = requests.get(url, params={"address": place_name, "key": key}, timeout=10)
    data = resp.json()
    if data.get("results"):
        loc = data["results"][0]["geometry"]["location"]
        return {"lat": loc["lat"], "lng": loc["lng"]}
    logging.warning("geocode_place: no results for %s", place_name)
    return None


# -------------------------
# Utilities
# -------------------------
def save_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, default=str, indent=2)
    logging.info("save_json: wrote %s", path)


def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


# Simple small helper to flatten nested dict one level (optional)
def flatten_one_level(d: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = json.dumps(v, default=str)
        else:
            out[k] = v
    return out

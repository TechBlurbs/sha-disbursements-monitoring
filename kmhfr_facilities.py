#!/usr/bin/env python3
import sys
import json
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://api.kmhfr.health.go.ke/api/facilities/facilities/"
OUT_CSV = Path("extracted_data/kmhfr_facilities.csv")
CKPT_JSON = Path("extracted_data/kmhfr_facilities.ckpt.json")
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X) KMHFR-Fetch/2.0"


def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=8,
        connect=5,
        read=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return s


def fetch(session: requests.Session, url: str, params: Optional[dict] = None) -> dict:
    r = session.get(url, params=params, timeout=60)
    # Manual handling if the server still returns a 500 after retry policy
    if r.status_code >= 500:
        # small extra backoff before giving up on this attempt
        time.sleep(2.5)
    r.raise_for_status()
    return r.json()


def flatten(f: dict) -> dict:
    def _get(obj, key, subkey="name"):
        v = obj.get(key)
        if isinstance(v, dict):
            return v.get(subkey)
        return f.get(f"{key}_name", v)

    return {
        "facility_id": f.get("id"),
        "code": f.get("code"),
        "name": f.get("name"),
        "official_name": f.get("official_name"),
        "abbreviation": f.get("abbreviation"),
        "keph_level": _get(f, "keph_level"),
        "facility_type": _get(f, "facility_type"),
        "owner": _get(f, "owner"),
        "owner_type": _get(f, "owner_type"),
        "operation_status": _get(f, "operation_status"),
        "county": _get(f, "county"),
        "sub_county": _get(f, "sub_county"),
        "constituency": _get(f, "constituency"),
        "ward": _get(f, "ward"),
        "latitude": f.get("latitude"),
        "longitude": f.get("longitude"),
        "town_name": f.get("town_name"),
        "nearest_landmark": f.get("nearest_landmark"),
        "active": f.get("active"),
        "approved": f.get("approved"),
        "is_published": f.get("is_published"),
        "updated": f.get("updated"),
    }


def clean_key(s: str) -> str:
    if s is None:
        return ""
    return (
        pd.Series([str(s).lower()])
        .str.normalize("NFKD")
        .str.encode("ascii", "ignore")
        .str.decode("ascii")
        .str.replace(r"[^a-z0-9]+", " ", regex=True)
        .str.replace(
            r"\b(hospital|dispensary|clinic|medical|centre|center|health|facility)\b",
            " ",
            regex=True,
        )
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .iloc[0]
    )


def save_ckpt(next_url: Optional[str], rows: int, out_path: Path):
    CKPT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(CKPT_JSON, "w") as f:
        json.dump({"next": next_url, "rows": rows, "csv": str(out_path)}, f)


def load_ckpt() -> Optional[dict]:
    if CKPT_JSON.exists():
        with open(CKPT_JSON) as f:
            return json.load(f)
    return None


def main():
    # CLI: python kmhfr_facilities_to_csv.py [out_csv] [--page-size N] [--resume]
    out = OUT_CSV
    page_size = None
    resume = False
    args = sys.argv[1:]
    i = 0
    if i < len(args) and not args[i].startswith("--"):
        out = Path(args[i])
        i += 1
    while i < len(args):
        if args[i] == "--page-size" and i + 1 < len(args):
            page_size = int(args[i + 1])
            i += 2
        elif args[i] == "--resume":
            resume = True
            i += 1
        else:
            print(f"Unknown arg: {args[i]}")
            sys.exit(2)

    out.parent.mkdir(parents=True, exist_ok=True)

    session = make_session()
    url = BASE_URL
    params = {}
    if page_size:
        params["page_size"] = page_size
    # Small filters can sometimes avoid heavy server paths; uncomment if needed:
    # params.update({"active": "true", "is_published": "true"})

    rows = []
    seen = set()

    if resume:
        ck = load_ckpt()
        if ck and ck.get("csv") == str(out) and ck.get("next"):
            url = ck["next"]
            params = None
            print(f"Resuming from checkpoint: {url}")

    pages = 0
    while url:
        try:
            data = fetch(session, url, params=params)
        except requests.HTTPError as e:
            # If the server barks, write what we have and exit non-zero
            if rows:
                df = pd.DataFrame(rows)
                df.to_csv(out, index=False)
                save_ckpt(url, len(rows), out)
            print(f"Fetch failed at: {url}\n{e}")
            sys.exit(1)

        results = data.get("results") or []
        for f in results:
            fid = f.get("id")
            if fid in seen:
                continue
            seen.add(fid)
            rows.append(flatten(f))

        url = data.get("next")
        params = None  # only for the first request
        pages += 1

        # checkpoint every 10 pages
        if pages % 10 == 0:
            df = pd.DataFrame(rows)
            df["facility_name_clean"] = (
                df["official_name"].fillna(df["name"]).map(clean_key)
            )
            df.drop_duplicates(subset=["facility_id"]).to_csv(out, index=False)
            save_ckpt(url, len(df), out)
            print(f"Checkpoint: {len(df)} rows, next={url}")

        # polite pacing
        time.sleep(0.4)

    # final write
    df = pd.DataFrame(rows)
    if df.empty:
        print("No facilities retrieved (server likely unstable).")
        sys.exit(3)
    df["facility_name_clean"] = df["official_name"].fillna(df["name"]).map(clean_key)
    df = df.drop_duplicates(subset=["facility_id"]).reset_index(drop=True)
    df.to_csv(out, index=False)
    save_ckpt(None, len(df), out)
    print(f"Wrote {len(df):,} facilities -> {out}")


if __name__ == "__main__":
    main()

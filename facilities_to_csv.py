#!/usr/bin/env python3
import json
import sys
from pathlib import Path

import requests
import pandas as pd

BASE_URL = "https://api.kmhfr.health.go.ke/api/facilities/facilities/"
OUT_CSV = sys.argv[1] if len(sys.argv) > 1 else "kmhfr_facilities.csv"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X) KMHFR-Fetch/1.0"


def fetch(url, params=None):
    r = requests.get(
        url, params=params or {}, headers={"User-Agent": USER_AGENT}, timeout=60
    )
    r.raise_for_status()
    return r.json()


def flatten_facility(f):
    """Pick common fields and lightly flatten nested structures that are present in KMHFR."""
    return {
        "id": f.get("id"),
        "code": f.get("code"),
        "name": f.get("name"),
        "official_name": f.get("official_name"),
        "abbreviation": f.get("abbreviation"),
        "registration_number": f.get("registration_number"),
        "facility_type_name": (
            f.get("facility_type_name") or f.get("facility_type", {}).get("name")
        ),
        "keph_level": (
            f.get("keph_level", {}).get("name")
            if isinstance(f.get("keph_level"), dict)
            else f.get("keph_level")
        ),
        "county": (
            f.get("county", {}).get("name")
            if isinstance(f.get("county"), dict)
            else f.get("county")
        ),
        "sub_county": (
            f.get("sub_county", {}).get("name")
            if isinstance(f.get("sub_county"), dict)
            else f.get("sub_county")
        ),
        "constituency": (
            f.get("constituency", {}).get("name")
            if isinstance(f.get("constituency"), dict)
            else f.get("constituency")
        ),
        "ward": (
            f.get("ward", {}).get("name")
            if isinstance(f.get("ward"), dict)
            else f.get("ward")
        ),
        "owner_type": f.get("owner_type_name") or (f.get("owner_type") or {}),
        "owner": f.get("owner_name") or (f.get("owner") or {}),
        "operation_status": f.get("operation_status_name")
        or (f.get("operation_status") or {}),
        "open_whole_day": f.get("open_whole_day"),
        "open_public_holidays": f.get("open_public_holidays"),
        "open_weekends": f.get("open_weekends"),
        "approved": f.get("approved"),
        "is_published": f.get("is_published"),
        "active": f.get("active"),
        "approved_national_level": f.get("approved_national_level"),
        "latitude": f.get("latitude"),
        "longitude": f.get("longitude"),
        "plot_number": f.get("plot_number"),
        "town_name": f.get("town_name"),
        "nearest_landmark": f.get("nearest_landmark"),
        "created": f.get("created"),
        "updated": f.get("updated"),
    }


def main():
    url = BASE_URL
    params = {}  # you can add filters if needed (e.g., {"county": "Nairobi"})
    rows = []
    seen = set()

    while url:
        data = fetch(url, params=params)
        results = data.get("results") or []
        for f in results:
            fid = f.get("id")
            if fid in seen:
                continue
            seen.add(fid)
            rows.append(flatten_facility(f))
        url = data.get("next")  # follow server-provided pagination
        params = None  # only include params on first call

    if not rows:
        print("No facilities retrieved.")
        return

    df = pd.DataFrame(rows)
    # Normalize nested dicts that may sneak in (owner_type/owner/operation_status fallbacks)
    for c in ["owner_type", "owner", "operation_status"]:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: x.get("name") if isinstance(x, dict) else x)

    # Dedup, sort, save
    df = df.drop_duplicates(subset=["id"]).sort_values(
        ["county", "sub_county", "name"], na_position="last"
    )
    df.to_csv(OUT_CSV, index=False)
    print(f"Wrote {len(df):,} facilities -> {OUT_CSV}")


if __name__ == "__main__":
    main()

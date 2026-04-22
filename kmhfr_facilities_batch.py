#!/usr/bin/env python3
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://api.kmhfr.health.go.ke/api"
COUNTIES_URL = f"{BASE}/common/counties/"
FACILITIES_URL = f"{BASE}/facilities/facilities/"

OUT_DIR = Path("extracted_data")
OUT_MERGED = OUT_DIR / "kmhfr_facilities.csv"
CKPT_JSON = OUT_DIR / "kmhfr_facilities_county.ckpt.json"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X) KMHFR-CountyBatch/1.0"

PAGE_SIZE = 100  # small pages survive better
PAUSE_BETWEEN_CALLS = 0.35


@dataclass
class Ckpt:
    county_id: Optional[str] = None
    county_name: Optional[str] = None
    next_url: Optional[str] = None
    finished_counties: List[str] = None


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


def fetch_json(
    session: requests.Session, url: str, params: Optional[dict] = None
) -> Dict[str, Any]:
    r = session.get(url, params=params or {}, timeout=60)
    if r.status_code >= 500:
        time.sleep(2.0)
    r.raise_for_status()
    return r.json()


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


def flatten_fac(f: dict) -> dict:
    def _name(o, key, sub="name"):
        v = o.get(key)
        if isinstance(v, dict):
            return v.get(sub)
        return o.get(f"{key}_name", v)

    return {
        "facility_id": f.get("id"),
        "code": f.get("code"),
        "name": f.get("name"),
        "official_name": f.get("official_name"),
        "abbreviation": f.get("abbreviation"),
        "keph_level": _name(f, "keph_level"),
        "facility_type": _name(f, "facility_type"),
        "owner": _name(f, "owner"),
        "owner_type": _name(f, "owner_type"),
        "operation_status": _name(f, "operation_status"),
        "county": _name(f, "county"),
        "sub_county": _name(f, "sub_county"),
        "constituency": _name(f, "constituency"),
        "ward": _name(f, "ward"),
        "latitude": f.get("latitude"),
        "longitude": f.get("longitude"),
        "town_name": f.get("town_name"),
        "nearest_landmark": f.get("nearest_landmark"),
        "active": f.get("active"),
        "approved": f.get("approved"),
        "is_published": f.get("is_published"),
        "updated": f.get("updated"),
    }


def save_ckpt(state: dict):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(CKPT_JSON, "w") as f:
        json.dump(state, f)


def load_ckpt() -> Optional[dict]:
    if CKPT_JSON.exists():
        with open(CKPT_JSON) as f:
            return json.load(f)
    return None


def per_county_path(county_name: str) -> Path:
    safe = "".join(ch if ch.isalnum() else "_" for ch in county_name.strip())
    return OUT_DIR / f"kmhfr_facilities_{safe}.csv"


def write_partial(rows: List[dict], path: Path):
    df = pd.DataFrame(rows)
    if df.empty:
        return
    df["facility_name_clean"] = df["official_name"].fillna(df["name"]).map(clean_key)
    df = df.drop_duplicates(subset=["facility_id"])
    df.to_csv(path, index=False)


def merge_all_partials() -> int:
    files = list(OUT_DIR.glob("kmhfr_facilities_*.csv"))
    if not files:
        return 0
    frames = []
    for f in files:
        try:
            frames.append(pd.read_csv(f))
        except Exception:
            continue
    if not frames:
        return 0
    merged = pd.concat(frames, ignore_index=True, sort=False)
    merged = merged.drop_duplicates(subset=["facility_id"])
    merged.to_csv(OUT_MERGED, index=False)
    return len(merged)


def list_counties(session: requests.Session) -> list[dict]:
    data = fetch_json(session, COUNTIES_URL)
    results = data.get("results") or data  # some endpoints return list directly
    # normalize to {id,name}
    out = []
    for c in results:
        if isinstance(c, dict) and c.get("id"):
            out.append(
                {
                    "id": c.get("id"),
                    "name": c.get("name") or c.get("county") or "Unknown",
                }
            )
    if not out:
        # fallback: try simple GET without pagination expectations
        raw = fetch_json(session, COUNTIES_URL)
        if isinstance(raw, list):
            for c in raw:
                if c.get("id"):
                    out.append({"id": c["id"], "name": c.get("name", "Unknown")})
    return out


def fetch_county(
    session: requests.Session,
    county_id: str,
    county_name: str,
    resume_next: Optional[str] = None,
) -> bool:
    """Return True if county completed; False if failed hard."""
    rows = []
    url = resume_next or FACILITIES_URL
    params = (
        None
        if resume_next
        else {
            "county": county_id,
            "active": "true",
            "is_published": "true",
            "page_size": PAGE_SIZE,
        }
    )

    while url:
        try:
            data = fetch_json(session, url, params=params)
        except requests.HTTPError as e:
            # log and return failure for this county
            print(f"[fail] {county_name} ({county_id}) at {url}: {e}")
            # write whatever we already have
            if rows:
                write_partial(rows, per_county_path(county_name))
            # save checkpoint so we can resume this county later
            save_ckpt(
                {
                    "county_id": county_id,
                    "county_name": county_name,
                    "next_url": url if isinstance(url, str) else None,
                    "finished_counties": [],
                }
            )
            return False

        results = data.get("results") or []
        for f in results:
            rows.append(flatten_fac(f))

        # progress checkpoint every couple pages
        if len(rows) and len(rows) % (PAGE_SIZE * 3) == 0:
            write_partial(rows, per_county_path(county_name))
            save_ckpt(
                {
                    "county_id": county_id,
                    "county_name": county_name,
                    "next_url": data.get("next"),
                    "finished_counties": [],
                }
            )
            print(f"[ckpt] {county_name}: {len(rows)}")

        url = data.get("next")
        params = None
        time.sleep(PAUSE_BETWEEN_CALLS)

    # final write for this county
    write_partial(rows, per_county_path(county_name))
    print(f"[done] {county_name}: {len(rows)} facilities")
    return True


def main():
    # CLI:
    #   python kmhfr_facilities_county_batch.py
    #   python kmhfr_facilities_county_batch.py --resume
    resume = "--resume" in sys.argv

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    session = make_session()

    # figure out where to resume
    ck = load_ckpt() if resume else None
    resume_county_id = ck.get("county_id") if ck else None
    resume_county_name = ck.get("county_name") if ck else None
    resume_next = ck.get("next_url") if ck else None
    finished = set(ck.get("finished_counties", [])) if ck else set()

    # list counties
    counties = list_counties(session)
    if not counties:
        print("No counties retrieved; the /common/counties/ endpoint may be down.")
        sys.exit(2)

    # if resuming inside a county, process that one first
    if resume_county_id and resume_county_name:
        print(f"Resuming county: {resume_county_name}")
        ok = fetch_county(session, resume_county_id, resume_county_name, resume_next)
        if ok:
            finished.add(resume_county_id)
            save_ckpt(
                {
                    "county_id": None,
                    "county_name": None,
                    "next_url": None,
                    "finished_counties": list(finished),
                }
            )

    # process the rest
    for c in counties:
        cid, cname = c["id"], c["name"]
        if cid in finished:
            continue
        # skip if we already have a per-county file (allows manual resume)
        out_path = per_county_path(cname)
        if out_path.exists() and out_path.stat().st_size > 128:
            print(f"[skip] {cname} (file exists)")
            finished.add(cid)
            save_ckpt(
                {
                    "county_id": None,
                    "county_name": None,
                    "next_url": None,
                    "finished_counties": list(finished),
                }
            )
            continue

        ok = fetch_county(session, cid, cname)
        if not ok:
            print(f"[warn] skipping {cname} due to errors; will need resume later.")
            # leave checkpoint pointing at next_url (saved by fetch_county)
            break
        finished.add(cid)
        save_ckpt(
            {
                "county_id": None,
                "county_name": None,
                "next_url": None,
                "finished_counties": list(finished),
            }
        )

    # merge partials
    total = merge_all_partials()
    if total == 0:
        print("No partials merged (API may be fully down).")
        sys.exit(3)
    print(f"[merged] {total} facilities -> {OUT_MERGED}")


if __name__ == "__main__":
    main()

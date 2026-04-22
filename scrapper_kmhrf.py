#!/usr/bin/env python3
"""
scrape_kmhfr_public.py — headless UI scrape of the KMHFR facilities table.

Requires:
  pip install playwright
  playwright install chromium

Usage:
  python scrape_kmhfr_public.py
"""
import json
import time
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

OUT = Path("extracted_data/kmhfr_ui_scrape.csv")
URL = "https://kmhfr.health.go.ke/public/facilities"


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()
        page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        # Give client-side loaders time to fetch first page
        page.wait_for_timeout(2500)

        # Heuristic selectors: adjust if KMHFR UI changes
        # Try to detect pagination count; else loop until "Next" disabled.
        def extract_page():
            # Run JS in the page to pull table rows; adapt selectors as needed
            js = """
            () => {
              const data = [];
              const table = document.querySelector('table');
              if (!table) return data;
              const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.innerText.trim());
              const idx = {};
              headers.forEach((h,i) => { idx[h.toLowerCase()] = i; });

              for (const tr of table.querySelectorAll('tbody tr')) {
                const tds = Array.from(tr.querySelectorAll('td')).map(td => td.innerText.trim());
                if (!tds.length) continue;
                data.push({
                  code: tds[idx['mfl code']] || tds[idx['code']] || null,
                  name: tds[idx['name']] || tds[idx['facility name']] || null,
                  county: tds[idx['county']] || null,
                  sub_county: tds[idx['sub county']] || tds[idx['subcounty']] || null,
                  facility_type: tds[idx['facility type']] || null,
                  keph_level: tds[idx['keph level']] || tds[idx['level']] || null
                });
              }
              return data;
            }
            """
            return page.evaluate(js)

        # pagination helpers
        def next_enabled():
            # look for a "Next" button that is not disabled
            btn = page.locator("text=Next")
            if not btn.count():
                return False
            cls = btn.first.get_attribute("class") or ""
            dis = "disabled" in cls.lower()
            return not dis

        # Loop pages
        page_no = 1
        while True:
            data = extract_page()
            rows.extend(data)
            # Try next page; several UIs use different buttons/aria
            moved = False
            for sel in [
                'button:has-text("Next")',
                'a:has-text("Next")',
                "[aria-label='Next']",
            ]:
                if page.locator(sel).count():
                    try:
                        page.locator(sel).first.click(timeout=5_000)
                        page.wait_for_timeout(1200)
                        moved = True
                        page_no += 1
                        break
                    except Exception:
                        continue
            if not moved:
                # Also break if next is disabled
                if not next_enabled():
                    break
                else:
                    break  # safety: stop if uncertain

        browser.close()

    df = pd.DataFrame(rows).drop_duplicates()
    df.to_csv(OUT, index=False)
    print(f"Wrote {len(df):,} rows -> {OUT}")


if __name__ == "__main__":
    main()

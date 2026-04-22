#!/usr/bin/env python3
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# -------- config --------
CATEGORY_URL = "https://sha.go.ke/resources/categories/11"
OUT_DIR = Path("sha_disbursements_pdfs")
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X) SHA-Downloader/2.0"
SLEEP = 0.8
TIMEOUT = 30
MAX_PAGES = 50
USE_PLAYWRIGHT = False  # set True if the site hides links behind JS

# Optional Playwright (pip install playwright && playwright install chromium)
try:
    from playwright.sync_api import sync_playwright  # noqa: F401
except Exception:
    sync_playwright = None
    USE_PLAYWRIGHT = True


# -------- helpers --------
def http_get(url, **kwargs):
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", USER_AGENT)
    headers.setdefault("Referer", CATEGORY_URL)
    r = requests.get(url, headers=headers, timeout=TIMEOUT, **kwargs)
    r.raise_for_status()
    return r


def norm_url(base, href):
    if not href:
        return None
    href = href.strip()
    if not href:
        return None
    return urljoin(base, href)


def looks_like_pdf(u: str) -> bool:
    if not u:
        return False
    low = u.lower()
    if low.endswith(".pdf"):
        return True
    # Some CMSes proxy downloads via routes like /download?file=...pdf
    if ".pdf" in low:
        return True
    return False


def safe_filename_from_url(url):
    name = os.path.basename(urlparse(url).path) or "file.pdf"
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    if not name.lower().endswith(".pdf"):
        # Try to preserve .pdf from query if present
        if ".pdf" in url.lower():
            name = (
                re.sub(
                    r"[^A-Za-z0-9._-]+",
                    "_",
                    url.lower().split(".pdf")[0].split("/")[-1],
                )
                + ".pdf"
            )
        else:
            name += ".pdf"
    return name


def download_file(url: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    fpath = out_dir / safe_filename_from_url(url)
    if fpath.exists() and fpath.stat().st_size > 0:
        return fpath
    with http_get(url, stream=True) as r, open(fpath, "wb") as f:
        for chunk in r.iter_content(1024 * 256):
            if chunk:
                f.write(chunk)
    return fpath


def render_html(url: str) -> str | None:
    if not USE_PLAYWRIGHT or not sync_playwright:
        return None
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            ctx = browser.new_context(user_agent=USER_AGENT)
            page = ctx.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT * 1000)
            page.wait_for_timeout(1500)
            return page.content()
        finally:
            browser.close()


def get_html(url: str) -> str | None:
    try:
        r = http_get(url)
        html = r.text
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code in (404, 410):
            return None
        raise
    # If you want to be aggressive with JS-rendered pages, uncomment below:
    # if USE_PLAYWRIGHT and "<script" in html.lower():
    #     rh = render_html(url)
    #     if rh:
    #         return rh
    return html


def iter_category_pages(category_url: str):
    # Page 1
    yield category_url
    # ?page=n
    for p in range(2, MAX_PAGES + 1):
        yield f"{category_url}?page={p}"
    # /page/n
    base = category_url.rstrip("/")
    for p in range(2, MAX_PAGES + 1):
        yield f"{base}/page/{p}"


def extract_resource_links(list_html: str, base_url: str) -> list[str]:
    """Find links to individual resource detail pages from a category listing."""
    soup = BeautifulSoup(list_html, "html.parser")
    links = set()
    for a in soup.find_all("a", href=True):
        href = norm_url(base_url, a["href"])
        if not href:
            continue
        # Heuristic: resource detail pages often look like /resources/<slug> or /resources/<id>/<slug>
        # Exclude category pages themselves.
        if (
            re.search(r"/resources/\d+|/resources/[^/]+$", href)
            and "categories" not in href
        ):
            links.add(href)
    return sorted(links)


def find_pdfs_in_detail_page(detail_html: str, base_url: str) -> list[str]:
    """Grab PDFs from the resource detail page (buttons/anchors)."""
    soup = BeautifulSoup(detail_html, "html.parser")
    pdfs = set()

    # 1) plain anchors
    for a in soup.find_all("a", href=True):
        full = norm_url(base_url, a["href"])
        if looks_like_pdf(full):
            pdfs.add(full)

    # 2) button-like elements with data-href
    for tag in soup.find_all(attrs={"data-href": True}):
        full = norm_url(base_url, tag.get("data-href"))
        if looks_like_pdf(full):
            pdfs.add(full)

    # 3) any <source> or <embed> with a PDF
    for tag in soup.find_all(["source", "embed"], src=True):
        full = norm_url(base_url, tag.get("src"))
        if looks_like_pdf(full):
            pdfs.add(full)

    return sorted(pdfs)


# -------- main --------
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)  # always create folder
    seen_detail = set()
    seen_pdf = set()
    download_count = 0

    # Step 1: crawl listing pages to collect resource detail URLs
    detail_urls = []
    for page_url in iter_category_pages(CATEGORY_URL):
        time.sleep(SLEEP)
        html = get_html(page_url)
        if not html:
            continue
        links = extract_resource_links(html, page_url)
        if not links and page_url != CATEGORY_URL:
            # No new items on this numbered page → stop probing that pattern
            continue
        for u in links:
            if u not in seen_detail:
                seen_detail.add(u)
                detail_urls.append(u)

    if not detail_urls:
        print("No resource detail links discovered from the category pages.")
        print(f"Checked base: {CATEGORY_URL}")
        print(f"Folder still created: {OUT_DIR.resolve()}")
        sys.exit(2)

    # Step 2: open each resource detail page and capture PDFs
    for detail in detail_urls:
        time.sleep(SLEEP)
        html = get_html(detail) or ""
        # As-needed JS render:
        if USE_PLAYWRIGHT and sync_playwright and not html.strip():
            html = render_html(detail) or ""
        pdfs = find_pdfs_in_detail_page(html, detail)
        if not pdfs:
            # Try a JS render if not already attempted
            if USE_PLAYWRIGHT and sync_playwright:
                rh = render_html(detail)
                if rh:
                    pdfs = find_pdfs_in_detail_page(rh, detail)

        for pdf_url in pdfs:
            if pdf_url in seen_pdf:
                continue
            seen_pdf.add(pdf_url)
            try:
                time.sleep(SLEEP)
                fpath = download_file(pdf_url, OUT_DIR)
                download_count += 1
                print(f"Downloaded: {pdf_url} -> {fpath.name}")
            except Exception as ex:
                print(f"Failed: {pdf_url} ({ex})")

    if download_count == 0:
        print("Finished with ZERO downloads. Likely causes:")
        print(
            "- SHA uses JS to inject links and you disabled Playwright (set USE_PLAYWRIGHT=True)."
        )
        print(
            "- PDFs are behind click handlers/redirectors not exposing '.pdf' in href."
        )
        print("- The category has no items yet or the site changed structure.")
        sys.exit(3)

    print(f"Done. {download_count} PDFs saved under: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()

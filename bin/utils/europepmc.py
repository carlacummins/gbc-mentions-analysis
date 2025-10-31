#!/usr/bin/env python3

from pprint import pprint
import sys
import os
import re
import glob
import gzip
import shutil

import random
from http.client import IncompleteRead
from urllib3.exceptions import ProtocolError
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

VERBOSE = False
retry_strategy = Retry(
    total=5,                      # Try up to 5 times
    connect=5,
    read=5,                       # retry on mid-stream read errors
    backoff_factor=1.5,           # Starts with 1.5s → 3s → 6s → 12s → 24s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    raise_on_status=False,
    respect_retry_after_header=True,
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)
session.headers.update({"User-Agent": "gbc-mentions/1.0"})

# query EuropePMC for publication metadata
max_retries = 5
epmc_base_url = "https://www.ebi.ac.uk/europepmc/webservices/rest"

def query_europepmc(endpoint, request_params=None, no_exit=False):
    """
    Query Europe PMC REST API endpoint with retries.
    """
    if not endpoint.startswith("http"):
        endpoint = f"{epmc_base_url}/{endpoint}"

    for attempt in range(max_retries):
        try:
            response = session.get(endpoint, params=request_params, timeout=15)
            if response.status_code == 200:
                return response.json() if 'json' in response.headers.get('Content-Type', '') else response.text
            else:
                if no_exit:
                    return None
                else:
                    sys.exit(f"Error: {response.status_code} for {endpoint}")
        except requests.RequestException as e:
            print(f"⚠️ Request failed: {e}. Retrying ({attempt + 1}/{max_retries})...")
    sys.exit("Max retries exceeded.")

def epmc_search(query, result_type='core', limit=0, cursor=None, returncursor=False, fields=[], page_size=1000):
    page_size = limit if (limit and limit <= page_size) else page_size

    all_results = []
    more_data = True
    while more_data:
        search_params = {
            'query': query, 'resultType': result_type,
            'format': 'json', 'pageSize': page_size,
            'cursorMark': cursor
        }
        data = query_europepmc(f"{epmc_base_url}/search", search_params)

        limit = limit if (limit > 0 and limit < data.get('hitCount')) else data.get('hitCount')
        if cursor is None and VERBOSE:
            print(f"-- Expecting {limit} of {data.get('hitCount')} results for query '{query}'!")


        if fields:
            restricted_results = []
            for result in data['resultList']['result']:
                restricted_results.append({k: result[k] for k in fields if k in result})
            data['resultList']['result'] = restricted_results

        all_results.extend(data['resultList']['result'])

        cursor = data.get('nextCursorMark')
        print(f"\t-- got {len(all_results)} results (cursor: {cursor})") if VERBOSE else None
        if not cursor:
            more_data = False

        if len(all_results) >= limit > 0:
            if VERBOSE:
                print(f"Reached limit of {limit} results, stopping.")
            more_data = False
            cursor = None  # reset cursor to avoid further queries

    return (all_results, cursor) if returncursor else all_results


# Robust .gz downloader with retries and gzip verification
def _download_gz_with_retry(url: str, dest_gz: str, max_attempts: int = 6, chunk_size: int = 1 << 20) -> str:
    """Download a .gz to dest_gz with retries and verify gzip integrity. Returns dest_gz on success."""
    os.makedirs(os.path.dirname(dest_gz), exist_ok=True)

    for attempt in range(1, max_attempts + 1):
        try:
            if VERBOSE:
                print(f"[ftp]\t⬇️ GET {url} → {dest_gz} (attempt {attempt}/{max_attempts})")
            # jitter to avoid thundering herd
            time.sleep(random.uniform(0, 0.25))

            with session.get(url, stream=True, timeout=(10, 180)) as r:
                r.raise_for_status()
                with open(dest_gz, "wb") as out:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            out.write(chunk)

            # Verify gzip integrity by fully reading
            with gzip.open(dest_gz, "rb") as gz:
                for _ in iter(lambda: gz.read(1 << 20), b""):
                    pass
            if VERBOSE:
                print(f"[ftp]\t✅ Downloaded and verified {dest_gz}")
            return dest_gz
        except (requests.RequestException, ProtocolError, IncompleteRead, OSError, gzip.BadGzipFile) as e:
            try:
                if os.path.exists(dest_gz):
                    os.remove(dest_gz)
            except OSError:
                pass
            if attempt == max_attempts:
                raise
            sleep_s = min(120, (2 ** (attempt - 1)) + random.uniform(0, 0.5))
            if VERBOSE:
                print(f"[ftp][retry] download failed: {e} — sleeping {sleep_s:.1f}s")
            time.sleep(sleep_s)

def _extract_article_from_combined_xml(big_xml, pmcid):
    """
    Given a combined XML path, extract the <article> block for the matching PMCID.
    Matches either of the following (whitespace/newlines tolerated):
      <article-id pub-id-type="pmcid">PMC7616738</article-id>
      <article-id pub-id-type="pmc">PMC7616738</article-id>
      <article-id pub-id-type="pmcid">7616738</article-id>
    """
    pmcid_num = str(pmcid[3:] if str(pmcid).startswith("PMC") else pmcid)
    pmcid_full = f"PMC{pmcid_num}"
    # Regex that tolerates attributes in any order and newlines/whitespace inside the tag.
    # We compile two patterns: one that expects the PMC prefix and one without, then test both.
    patt_with_prefix = re.compile(
        rf"<article-id[^>]*pub-id-type=\"pmc(?:id)?\"[^>]*>\s*{re.escape(pmcid_full)}\s*</article-id>",
        re.IGNORECASE | re.DOTALL,
    )
    patt_no_prefix = re.compile(
        rf"<article-id[^>]*pub-id-type=\"pmc(?:id)?\"[^>]*>\s*{re.escape(pmcid_num)}\s*</article-id>",
        re.IGNORECASE | re.DOTALL,
    )

    inside_article = False
    buffer = []
    with open(big_xml, "r", encoding="utf-8") as infile:
        for line in infile:
            if not inside_article and "<article" in line:
                inside_article = True
                buffer = [line]
                continue
            if inside_article:
                buffer.append(line)
                if "</article>" in line:
                    article_xml = "".join(buffer)
                    if patt_with_prefix.search(article_xml) or patt_no_prefix.search(article_xml):
                        if VERBOSE:
                            print(f"\t✅ Matched PMCID {pmcid_full} in {os.path.basename(big_xml)}")
                        return article_xml
                    inside_article = False
                    buffer = []

    sys.stderr.write(f"PMCID {pmcid_num} not found in {big_xml}\n")
    return None

def _safe_samefile(a, b):
    try:
        return os.path.samefile(a, b)
    except FileNotFoundError:
        return False

def _ensure_decompressed(gz_path: str, xml_path: str):
    """Decompress only if xml doesn't exist or gz is newer."""
    if os.path.exists(xml_path) and os.path.getmtime(xml_path) >= os.path.getmtime(gz_path):
        return xml_path
    with gzip.open(gz_path, 'rb') as src, open(xml_path, 'wb') as out_f:
        shutil.copyfileobj(src, out_f)
    return xml_path


# cache of per-path indices: { path: { 'PMC12345': '/path/PMC12345_PMC12399.xml[.gz]' } }
pmc_file_index_by_path = {}

def _find_local_fulltext(pmcid, path, dest='/tmp'):
    """
    Given a path to a directory containing Europe PMC XML files,
    find the full text XML for a given PMCID.

    The files each contain multiple articles and are named like "PMC123456_PMC123999.xml.gz" or "PMC123456_PMC123999.xml",
    as provided by Europe PMC : <https://europepmc.org/ftp/oa/>

    Identify the correct file by checking the PMCID range in the filename,
    then extract and return the matching article.
    """
    global pmc_file_index_by_path
    pmcid = f"PMC{pmcid[3:]}" if str(pmcid).startswith("PMC") else f"PMC{pmcid}"

    # ensure we have an index for this path, and index both .xml and .xml.gz bundles
    index = pmc_file_index_by_path.get(path)
    print(f"[local] files indexed for {path}:", set(index.values()) if index else []) if VERBOSE else None
    f = index.get(pmcid) if index else None

    if not index or not f:
        print(f"[local] Building index for {path}") if VERBOSE else None
        index = {}
        for f in glob.glob(os.path.join(path, "PMC*_PMC*.xml*")):
            base = os.path.basename(f)
            m = re.match(r'^PMC(\d+)_PMC(\d+)\.xml(?:\.gz)?$', base)
            if not m:
                print(f"[local]\t❌ Skipping {base} - does not match expected pattern") if VERBOSE else None
                continue
            start, end = map(int, m.groups())
            for x in range(start, end + 1):
                index[f"PMC{x}"] = f
        pmc_file_index_by_path[path] = index

        f = index.get(pmcid)

    if not f:
        print(f"[local]\t❌ No matching file found for PMCID {pmcid} in {path}") if VERBOSE else None
        return None
    else:
        print(f"[local]\t✅ Found bundle {os.path.basename(f)} for {pmcid}") if VERBOSE else None

    # If it's a .gz, decompress into dest only if needed
    if f.endswith('.gz'):
        gz_dest = f if os.path.dirname(f) == dest else os.path.join(dest, os.path.basename(f))
        if not _safe_samefile(f, gz_dest):
            os.makedirs(dest, exist_ok=True)
            print(f"[local]\t📥 Caching {f} → {gz_dest}") if VERBOSE else None
            if os.path.exists(f) and os.path.dirname(f) != dest:
                shutil.copy2(f, gz_dest)
            else:
                # if f is already in dest, just keep gz_dest = f
                pass
        xml_path = os.path.join(dest, os.path.basename(f)[:-3])
        f_xml = _ensure_decompressed(gz_dest, xml_path)
    else:
        # plain .xml: copy only if different location
        if os.path.dirname(f) != dest:
            copied = os.path.join(dest, os.path.basename(f))
            if not _safe_samefile(f, copied):
                os.makedirs(dest, exist_ok=True)
                if VERBOSE: print(f"[local]\t📥 Copying {f} → {copied}")
                shutil.copy2(f, copied)
            f_xml = copied
        else:
            f_xml = f

    pmcid_num = int(pmcid[3:])
    return _extract_article_from_combined_xml(f_xml, pmcid_num)


_epmc_index = None

def _get_epmc_index(ftp_address="https://europepmc.org/pub/databases/pmc/oa/"):
    global _epmc_index
    if _epmc_index is not None:
        return _epmc_index
    r = session.get(ftp_address, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    idx = []
    for a in soup.find_all("a"):
        href = a.get("href")
        m = re.match(r'PMC(\d+)_PMC(\d+)\.xml\.gz$', href or "")
        if m:
            idx.append((int(m.group(1)), int(m.group(2)), href))
    idx.sort()
    _epmc_index = (ftp_address.rstrip("/"), idx)
    return _epmc_index

def _find_europepmc_ftp_fulltext(pmcid, ftp_address="https://europepmc.org/pub/databases/pmc/oa/", dest='/tmp'):
    """
    Given the HTML address of the Europe PMC FTP, find the full text XML for a given PMCID.

    The files each contain multiple articles and are named like "PMC123456_PMC123999.xml.gz",
    as provided by Europe PMC : <https://europepmc.org/pub/databases/pmc/oa/>

    Identify the correct file by checking the PMCID range in the filename,
    then extract and return the matching article.
    """
    pmcid_num = pmcid[3:] if str(pmcid).startswith("PMC") else pmcid  # remove 'PMC' prefix
    pmcid_num = int(pmcid[3:] if str(pmcid).startswith("PMC") else pmcid)
    base, idx = _get_epmc_index(ftp_address)

    # pick the single bundle containing pmcid_num
    pmc_file = None
    # binary search would be nicer, but linear is fine once per task
    for start, end, fname in idx:
        if start <= pmcid_num <= end:
            pmc_file = fname
            break
    if not pmc_file:
        print(f"[ftp]\t❌ No matching file found for PMCID {pmcid}") if VERBOSE else None
        return None

    gz_dest  = os.path.join(dest, pmc_file)
    xml_dest = os.path.join(dest, pmc_file[:-3])
    os.makedirs(dest, exist_ok=True)

    # download only if missing
    if not os.path.exists(gz_dest) or os.path.getsize(gz_dest) == 0:
        if VERBOSE: print(f"[ftp]\t📥 Downloading {base}/{pmc_file} → {gz_dest}")
        _download_gz_with_retry(f"{base}/{pmc_file}", gz_dest)

    # decompress only if needed
    xml_path = _ensure_decompressed(gz_dest, xml_dest)
    os.remove(gz_dest)  # remove the .gz file after decompression

    return _extract_article_from_combined_xml(xml_path, pmcid_num)

def get_fulltext_body(pmcid, path=None, dest='/tmp'):
    """
    Fetch the full text body of a publication from Europe PMC by PMCID.
    If a local path is provided, it will first check for a local XML file.
    If not found, it will download the XML from Europe PMC.
    """
    xml = None
    path = path or dest # check for files downloaded from FTP or local XMLs
    if path:
        # find the matching record in the filesystem
        if VERBOSE: print(f"[local] Searching {path} for full text XML for {pmcid}")
        xml = _find_local_fulltext(pmcid, path, dest=dest)

    if not xml:
        # if not found locally, try the EuropePMC FTP
        if VERBOSE: print(f"[ftp] Searching EuropePMC FTP for full text XML for {pmcid}")
        xml = _find_europepmc_ftp_fulltext(pmcid, dest=dest)

    if not xml:
        # 1. Download the XML
        if VERBOSE: print(f"[api] Querying EuropePMC's API for full text XML for {pmcid}")
        url = f"{epmc_base_url}/{pmcid}/fullTextXML"
        response = requests.get(url)
        if response.status_code != 200:
            return (None, None)
        xml = response.text

    if not xml:
        return (None, None)

    # 2. Parse with BeautifulSoup
    if VERBOSE:
        print("\n🎉 XML found! Parsing text and tables from XML body")
    soup = BeautifulSoup(xml, "lxml-xml")

    # 3. Extract body text with headers
    text_blocks = []

    # 1. Title
    title = soup.find("article-title")
    if title:
        title_text = title.get_text(strip=True)
        if title_text:
            text_blocks.append(f"# TITLE\n{title_text}")
    text_blocks.append("\n")

    # 2. Abstract
    abstract = soup.find("abstract")
    if abstract:
        abstract_title = abstract.find("title")
        if abstract_title and abstract_title.get_text(strip=True).upper() == 'ABSTRACT':
            abstract_title.extract()  # remove the title

        text_blocks.append(f"# ABSTRACT\n{_section_to_text(abstract)}")

    # 2.1. Other metadata sections
    funding_statement = soup.find("funding-statement")
    if funding_statement:
        funding_text = funding_statement.get_text(strip=True)
        if funding_text:
            text_blocks.append(f"### FUNDING\n{funding_text}")

    all_custom_metas = soup.find_all("custom-meta")
    for custom_meta in all_custom_metas:
        meta_name = custom_meta.find("meta-name").get_text(strip=True)
        meta_value = custom_meta.find("meta-value").get_text(strip=True)
        if meta_name and meta_value:
            text_blocks.append(f"### {meta_name.upper()}\n{meta_value}")

    text_blocks.append("\n")

    # 3. Tables (captions + content)
    table_blocks = []
    for tbl in soup.find_all("table-wrap"):
        tbl.extract()
        processed_table = _preprocess_xml_table(tbl)
        if processed_table:
            table_blocks.append(processed_table)

    # 4. Main body (sections + paragraphs)
    # excluded_section_types = ["supplementary-material", "orcid"]
    excluded_section_types = ["orcid"]
    body = soup.find("body")
    if body:
        all_sections = body.find_all("sec", recursive=False)
        for elem in all_sections:
            if elem.get("sec-type") in excluded_section_types:
                continue

            text_blocks.append(_section_to_text(elem))
            text_blocks.append("\n")

    return text_blocks, table_blocks

def _section_to_text(section, depth=1):
    """Converts a BeautifulSoup section to a string."""
    text = []
    title = section.find("title", recursive=False)
    if title:
        text.append(f"{'#'*depth} {title.get_text(strip=True).upper()}")

    elems = section.find_all(["sec", "p"], recursive=False) # only direct children
    for elem in elems:
        if elem.name == "sec":
            text.append(_section_to_text(elem, depth=(depth+1)))
        elif elem.name == "p":
            # check for embedded lists
            plists = elem.find_all("list", recursive=False)
            for plist in plists:
                for li in elem.find_all("list-item", recursive=True):
                    li_text = li.get_text(strip=True)
                    if li_text:
                        text.append(f"- {li_text}.")

                plist.extract() # remove the lists from the main paragraph

            p_text = elem.get_text(strip=True)
            if p_text:
                text.append(p_text)

    return "\n".join(text) if text else ''

def _preprocess_xml_table(table_wrap_tag):
    """Extracts and flattens a single <table-wrap> tag into a list of text lines suitable for NER."""
    lines = []

    # Caption
    caption = table_wrap_tag.find("caption")
    if caption:
        cap_text = caption.get_text(strip=True)
        if cap_text:
            lines.append(f"[TABLE-CAPTION] {cap_text}")

    # Table body
    table = table_wrap_tag.find("table")
    if table:
        rows = table.find_all("tr")
        for i, row in enumerate(rows):
            cells = row.find_all(["td", "th"])
            if cells:
                row_text = []
                for cell in cells:
                    text = cell.get_text(strip=True)
                    if text:
                        is_header = cell.name == "th" or i == 0
                        prefix = "[COLUMN-HEADER] " if is_header else ""
                        row_text.append(f"{prefix}{text}")
                if row_text:
                    lines.append(" | ".join(row_text))

    return ".\n".join(lines) if lines else None # include . for better sentence tokenization

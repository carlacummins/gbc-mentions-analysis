#!/usr/bin/env python3

import sys
import os
import re
import glob
import gzip
import shutil

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from lxml import etree

retry_strategy = Retry(
    total=5,                      # Try up to 5 times
    backoff_factor=1.5,           # Starts with 1.5s → 3s → 6s → 12s → 24s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    raise_on_status=False
)

adapter = HTTPAdapter(max_retries=retry_strategy)
session = requests.Session()
session.mount("https://", adapter)
session.mount("http://", adapter)

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

def epmc_search(query, result_type='core', limit=0, cursor=None, returncursor=False, fields=[]):
    page_size = limit if (limit and limit <= 1000) else 1000

    all_results = []
    more_data = True
    while more_data:
        search_params = {
            'query': query, 'resultType': result_type,
            'format': 'json', 'pageSize': page_size,
            'cursorMark': cursor
        }
        data = query_europepmc(f"{epmc_base_url}/search", search_params)

        limit = limit or data.get('hitCount')
        if cursor is None:
            print(f"----- Expecting {limit} of {data.get('hitCount')} results!")

        if fields:
            restricted_results = []
            for result in data['resultList']['result']:
                restricted_results.append({k: result[k] for k in fields if k in result})
            data['resultList']['result'] = restricted_results

        all_results.extend(data['resultList']['result'])
        print(f"got {len(all_results)} results")

        cursor = data.get('nextCursorMark')
        if not cursor:
            more_data = False

        if len(all_results) >= limit > 0:
            print(f"Reached limit of {limit} results, stopping.")
            more_data = False

    return (all_results, cursor) if returncursor else all_results

def _extract_article_from_combined_xml(xml_content, pmcid):
    """
    Given a combined XML content, extract the article with the matching PMCID.
    """

    pmcid_num = pmcid[3:] if pmcid.startswith("PMC") else pmcid  # remove 'PMC' prefix

    # Parse with lxml (much faster than bs4)
    root = etree.fromstring(xml_content.encode("utf-8"))

    # XPath: find the article that has an <article-id pub-id-type="pmcid"> with the matching number
    # This directly traverses the tree, no manual loops.
    match = root.xpath(f".//article[article-meta/article-id[@pub-id-type='pmcid' = '{pmcid_num}']]")
    if match:
        # return string version of the first match
        return etree.tostring(match[0], encoding="unicode")
    else:
        return None

pmc_file_lookup = {}
def _find_local_fulltext(pmcid, path):
    """
    Given a path to a directory containing Europe PMC XML files,
    find the full text XML for a given PMCID.

    The files each contain multiple articles and are named like "PMC123456_PMC123999.xml.gz",
    as provided by Europe PMC : <https://europepmc.org/ftp/oa/>

    Identify the correct file by checking the PMCID range in the filename,
    then extract and return the matching article.
    """

    if pmcid not in pmc_file_lookup:
        # build a lookup table for matching files in the directory
        files = glob.glob(f"{path}/*{pmcid[:4]}*.xml*", recursive=False)
        for f in files:
            xml_range = re.search(r'PMC(\d+)_PMC(\d+)\.xml', f)
            if xml_range:
                start, end = map(int, xml_range.groups())
                x = start
                while x <= end:
                    pmc_file_lookup[f"PMC{x}"] = f
                    x += 1

    if pmcid in pmc_file_lookup:
        f = pmc_file_lookup[pmcid]
        pmcid_num = int(pmcid[3:]) # remove 'PMC' prefix and convert to int

        if f.endswith('.gz'): # if gzipped, decompress to a temporary file
            uncompressed = f"/tmp/{os.path.basename(f[:-3])}"
            with gzip.open(f, 'rb') as src, open(uncompressed, 'wb') as dst:
                shutil.copyfileobj(src, dst)
            f = uncompressed
        elif f.endswith('.xml'): # otherwise, copy to a temporary location
            copied = f"/tmp/{os.path.basename(f)}"
            os.system(f"cp {f} {copied}")
            f = copied
        with open(f, 'r', encoding='utf-8') as xml_file:
            xml_content = xml_file.read()
        os.remove(f)  # remove the temporary file if it was created

        return _extract_article_from_combined_xml(xml_content, pmcid_num)
        # soup = BeautifulSoup(xml_content, "lxml-xml")
        # all_articles = soup.find_all("article")
        # if (end-pmcid_num) > (pmcid_num-start):
        #     # if the pmcid is closer to the end, search from the end
        #     all_articles.reverse()

        # for article in all_articles:
        #     pmcid_tag = article.find("article-id", {"pub-id-type": "pmcid"})
        #     if pmcid_tag and pmcid_tag.get_text(strip=True) == str(pmcid_num):
        #         return str(article)

    return None

def get_fulltext_body(pmcid, path=None):
    """
    Fetch the full text body of a publication from Europe PMC by PMCID.
    If a local path is provided, it will first check for a local XML file.
    If not found, it will download the XML from Europe PMC.
    """
    xml = None
    if path:
        # find the matching record in the filesystem
        xml = _find_local_fulltext(pmcid, path)

    if not xml:
        # 1. Download the XML
        url = f"{epmc_base_url}/{pmcid}/fullTextXML"
        response = requests.get(url)
        if response.status_code != 200:
            return None
        xml = response.text

    # 2. Parse with BeautifulSoup
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
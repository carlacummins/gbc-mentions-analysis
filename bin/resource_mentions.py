#!/usr/bin/env python3

import json
import sys
from tabulate import tabulate

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from bs4 import BeautifulSoup

# setup retry strategy and HTTP adapter to handle rate limiting
# and transient errors
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
    for attempt in range(max_retries):
        try:
            response = session.get(endpoint, params=request_params, timeout=15)
            if response.status_code == 200:
                return response.json() if 'json' in response.headers.get('Content-Type', '') else response.text
            else:
                # print(f"⚠️ Error: {response.status_code} for {endpoint}")
                if no_exit:
                    return None
                else:
                    sys.exit(f"Error: {response.status_code} for {endpoint}")
        except requests.RequestException as e:
            print(f"⚠️ Request failed: {e}. Retrying ({attempt + 1}/{max_retries})...")
    sys.exit("Max retries exceeded.")

def find_gbcr_publications(gcbr_names, gcbr_topics, return_count=False):
    """
    Find all publications in Europe PMC that match the GCBR name.
    """
    query = f'''(
    ({' OR '.join(f"'{g}'" for g in gcbr_names)}) AND
    {f'({' OR '.join(f"'{t}'" for t in gcbr_topics)}) AND' if gcbr_topics else ''}
    PUB_TYPE:journal-article
    )'''

    params = {
        'query': query,
        'format': 'json',
        'pageSize': 1,
        'cursorMark': '*',
        'resultType': 'core'
    }

    # print(f"🔍 Querying Europe PMC with: {query}")
    data = query_europepmc(f"{epmc_base_url}/search", request_params=params)

    if not data:
        sys.exit("No publications found or an error occurred.")

    # print(f"🔍 Found {data['hitCount']} publications matching resource names.")
    if return_count:
        return data['hitCount']

    if 'resultList' in data and 'result' in data['resultList']:
        results = data['resultList']['result']
        return results
    else:
        return []

def get_fulltext_body(pmcid):
    # 1. Download the XML
    url = f"{epmc_base_url}/{pmcid}/fullTextXML"
    response = requests.get(url)
    if response.status_code != 200:
        # print(f"⚠️ Error for {pmcid}: bad code: {response.status_code}")
        return None
    xml = response.text

    # 2. Parse with BeautifulSoup
    soup = BeautifulSoup(xml, "lxml-xml")

    # 3. Extract body text with headers
    body = soup.find("body")
    text_blocks = []
    if body:
        for elem in body.find_all(["sec", "p"], recursive=True):
            if elem.name == "sec":
                title = elem.find("title")
                if title:
                    text_blocks.append(f"[SECTION] {title.get_text(strip=True)}")
            elif elem.name == "p":
                text_blocks.append(elem.get_text(strip=True))

    # 4. Extract tables
    tables = soup.find_all("table-wrap")
    table_blocks = []

    for tbl in tables:
        lines = []

        # Caption
        caption = tbl.find("caption")
        if caption:
            lines.append(f"[TABLE-CAPTION] {caption.get_text(strip=True)}")

        # Table headers + rows
        table = tbl.find("table")
        if table:
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                if cells:
                    row_text = " ".join(cell.get_text(strip=True) for cell in cells)
                    lines.append(row_text)

        if lines:
            table_blocks.append("\n".join(lines))

    return (text_blocks, table_blocks)

def get_resource_mentions(textblocks, tableblocks, resource_names):
    mentions = []

    # print(f"🔍 Searching for mentions of resources: {', '.join(resource_names)}"
    #       f" in {len(textblocks)} text blocks and {len(tableblocks)} table blocks.")
    # print(textblocks)

    # Split the fulltext into sentences and table rows
    for block in textblocks:
        sentences = block.split('. ')
        for sentence in sentences:
            for resource_name in resource_names:
                if resource_name in sentence:
                    mentions.append(sentence.strip())
    for table in tableblocks:
        rows = table.split('\n')
        for row in rows:
            for resource_name in resource_names:
                if resource_name in row:
                    mentions.append(row.strip())

    # Remove duplicates
    mentions = list(set(mentions))
    # Remove empty mentions
    mentions = [m for m in mentions if m.strip()]

    return mentions

def main():
    resource_info = json.loads(open(sys.argv[1], 'r').read())
    resource_counts = []

    for resource in resource_info:
        publications = find_gbcr_publications(resource['names'], resource.get('topics', []), return_count=True)
        resource_counts.append((resource['names'][0], publications))

    print(resource_counts)
    resource_counts.append(("total", sum([x[1] for x in resource_counts])))

    # Format numbers with commas in resource_counts
    formatted_counts = [(x[0], f"{x[1]:,}") for x in resource_counts]
    print(tabulate(formatted_counts, headers=["Resource", "Publication Count"], tablefmt="grid"))
    print("Done processing resource publications.")

if __name__ == "__main__":
    main()
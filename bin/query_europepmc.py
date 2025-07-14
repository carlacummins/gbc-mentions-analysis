#!/usr/bin/env python3

import json
import math
import argparse
import os
import sqlalchemy as db

from utils.gbc_db import get_gbc_connection
from utils.europepmc import epmc_search

parser = argparse.ArgumentParser(description="Query Europe PMC for resource mentions.")
parser.add_argument('--outdir', type=str, required=True, help='Output directory for results')
parser.add_argument('--chunks', type=int, default=1, help='Number of chunks to split the work into')
args = parser.parse_args()

os.makedirs(args.outdir, exist_ok=True)

gcp_connector, db_engine, db_conn = get_gbc_connection(test=False)
# db_conn = db_engine.connect()

resources_sql = "SELECT short_name, common_name, full_name FROM resource WHERE is_latest= limit 3"
resource_aliases = db_conn.execute(db.text(resources_sql)).fetchall()

articles = {}
for r in resource_aliases:
    ras = list(set(r))
    epmc_query = f"(SRC:MED OR SRC:PMC) AND ({" OR ".join([f'"{alias}"' for alias in ras if alias])})"  # Join aliases with OR for EPMC query
    print(f"Searching Europe PMC for: {epmc_query}")
    results = epmc_search(epmc_query, result_type='core')

    for article in results:
        this_pmcid = article.get('pmcid')
        this_pmid = article.get('pmid')

        this_id = this_pmcid or this_pmid
        if not this_id:
            continue

        if this_id not in articles:
            # Initialize the article entry if it doesn't exist
            # Save only metadata required for the database entry
            articles[this_id] = {
                'pmcid': this_pmcid,
                'pmid': this_pmid,
                'title': article.get('title'),
                'firstPublicationDate': article.get('journalInfo', {}).get('printPublicationDate') or article.get('firstPublicationDate'),
                'authorString': article.get('authorString', ''),
                'authorList': article.get('authorList', {}),
                'citedByCount': article.get('citedByCount', 0),
                'grantsList': article.get('grantsList', {}),
                "keywordList": article.get('keywordList', {}),
                'meshHeadingList': article.get('meshHeadingList', {}),
            }


def split_dict_to_json_chunks(data, num_chunks, base_filename="chunk"):
    # Convert dictionary items to list for slicing
    items = list(data.items())
    chunk_size = math.ceil(len(items) / num_chunks)

    for i in range(num_chunks):
        chunk_dict = dict(items[i*chunk_size : (i+1)*chunk_size])
        with open(f"{base_filename}_{i+1}.json", "w") as f:
            json.dump(chunk_dict, f)

split_dict_to_json_chunks(articles, args.chunks, os.path.join(args.outdir, "epmc_articles"))
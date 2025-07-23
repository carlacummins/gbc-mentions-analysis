#!/usr/bin/env python3

import json
import argparse
import os

from utils.europepmc import epmc_search

parser = argparse.ArgumentParser(description="Query Europe PMC for resource mentions.")
parser.add_argument('--outdir', type=str, required=True, help='Output directory for results')
parser.add_argument('--resources', type=str, required=True, help='JSON file containing resource names and aliases')
parser.add_argument('--chunks', type=int, default=1, help='Number of chunks to split the work into')
parser.add_argument('--epmc_limit', type=int, default=0, help='Limit for the number of results to fetch from Europe PMC')

args = parser.parse_args()

os.makedirs(args.outdir, exist_ok=True)
resource_aliases = json.load(open(args.resources))

articles_metadata = {}
for r in resource_aliases:
    ras = list(set(r))
    epmc_query = f"(SRC:PMC) AND ({" OR ".join([f'"{alias}"' for alias in ras if alias])})"  # Join aliases with OR for EPMC query
    print(f"Searching Europe PMC for: {epmc_query}")
    results = epmc_search(epmc_query, result_type='core', limit=args.epmc_limit)

    for article in results:
        this_pmcid = article.get('pmcid')
        this_pmid = article.get('pmid')

        this_id = this_pmcid or this_pmid
        if not this_id:
            continue

        if this_id not in articles_metadata:
            # Initialize the article entry if it doesn't exist
            # Save only metadata required for the database entry
            articles_metadata[this_id] = {
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


# Split the idlist into chunks
idlist = [a['pmcid'] for a in articles_metadata.values()]
k, m = divmod(len(idlist), args.chunks)
idlist_chunks = [idlist[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(args.chunks)]
for i, chunk in enumerate(idlist_chunks):
    chunk_file = os.path.join(args.outdir, f"pmc_idlist.chunk_{i+1}.txt")
    with open(chunk_file, 'w') as f:
        f.write("\n".join(chunk))
    print(f"Saved chunk {i+1} with {len(chunk)} IDs to {chunk_file}")

# Save the metadata to a JSON file
metadata_file = os.path.join(args.outdir, "article_metadata.json")
with open(metadata_file, 'w') as f:
    json.dump(articles_metadata, f, indent=2)
    print(f"Saved article metadata to {metadata_file}")
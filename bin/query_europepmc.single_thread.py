#!/usr/bin/env python3

import json
import argparse
import os
import gzip

from gbcutils.europepmc import epmc_search
from gbcutils.metadata import shard_key, shard_path

parser = argparse.ArgumentParser(description="Query Europe PMC for resource mentions.")
parser.add_argument('--outdir', type=str, required=True, help='Output directory for results')
parser.add_argument('--resources', type=str, required=True, help='JSON file containing resource names and aliases')
parser.add_argument('--chunks', type=int, default=1, help='Number of chunks to split the work into')
parser.add_argument('--epmc_limit', type=int, default=0, help='Limit for the number of results to fetch from Europe PMC')
parser.add_argument('--shards', type=int, default=128, help='Number of JSONL shards to write for metadata')
parser.add_argument('--verbose', action='store_true', help='Enable verbose output')

args = parser.parse_args()

os.makedirs(args.outdir, exist_ok=True)
resource_aliases = json.load(open(args.resources))

metadata_outdir = os.path.join(args.outdir, "metadata")
os.makedirs(metadata_outdir, exist_ok=True)

# sharded JSONL writers
writers = {}
def get_writer(k: int):
    if k not in writers:
        writers[k] = gzip.open(shard_path(k, basepath=metadata_outdir, shards=args.shards), 'at', encoding='utf-8')
    return writers[k]

articles_metadata = {}
for r in resource_aliases.values():
    ras = list(set(r))
    joined_aliases = " OR ".join(f'"{alias}"' for alias in ras if alias)
    epmc_query = f"(HAS_FT:Y) AND ({joined_aliases})"
    if args.verbose:
        print(f"Searching Europe PMC for: {epmc_query}")
    results = epmc_search(epmc_query, result_type='core', limit=args.epmc_limit)

    for article in results:
        this_pmcid = article.get('pmcid')

        if not this_pmcid:
            continue

        if this_pmcid not in articles_metadata:
            # Initialize the article entry if it doesn't exist
            # Save only metadata required for the database entry
            articles_metadata[this_pmcid] = 1
            this_article_metadata = {
                'id': this_pmcid,
                'pmcid': this_pmcid,
                'pmid': article.get('pmid'),
                'title': article.get('title'),
                'firstPublicationDate': article.get('journalInfo', {}).get('printPublicationDate') or article.get('firstPublicationDate'),
                'authorString': article.get('authorString', ''),
                'authorList': article.get('authorList', {}),
                'citedByCount': article.get('citedByCount', 0),
                'grantsList': article.get('grantsList', {}),
                "keywordList": article.get('keywordList', {}),
                'meshHeadingList': article.get('meshHeadingList', {}),
            }

            k = shard_key(this_pmcid, args.shards)
            get_writer(k).write(json.dumps(this_article_metadata, ensure_ascii=False) + "\n")
            if args.verbose:
                print(f"Appended metadata for {this_pmcid} to {shard_path(k)}")

for fh in writers.values():
    fh.close()

# Split the idlist into chunks
# idlist = [a['pmcid'] for a in articles_metadata.values()]
idlist = list(articles_metadata.keys())
idlist.sort()
k, m = divmod(len(idlist), args.chunks)
idlist_chunks = [idlist[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(args.chunks)]
for i, chunk in enumerate(idlist_chunks):
    chunk_file = os.path.join(args.outdir, f"pmc_idlist.chunk_{i+1}.txt")
    with open(chunk_file, 'w') as f:
        f.write("\n".join(chunk))
    if args.verbose:
        print(f"Saved chunk {i+1} with {len(chunk)} IDs to {chunk_file}")

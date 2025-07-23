#!/usr/bin/env python3

import argparse
from utils.europepmc import get_fulltext_body

parser = argparse.ArgumentParser(description="Fetch and preprocess articles.")
parser.add_argument('--pmcid', help='PMC ID of the article')
parser.add_argument('--idlist', help='Path to file containing list of PMC IDs')
parser.add_argument('--outdir', help='Directory to write output files', default='pmc_preprocessed')
parser.add_argument('--local_xml_dir', help='Directory containing local XML files', default=None)
args = parser.parse_args()

ids = []
if args.pmcid:
    ids = [args.pmcid]
elif args.idlist:
    with open(args.idlist, 'r') as f:
        ids = [line.strip() for line in f if line.strip()]
else:
    raise ValueError("You must provide either a PMC ID (--pmcid) or a file containing a list of PMC IDs (--idlist).")

for pmcid in ids:
    this_outfile = open(f"{args.outdir}/{pmcid}.txt", 'w')
    text_blocks, table_blocks = get_fulltext_body(pmcid, path=args.local_xml_dir) # fetch and parse the full text body
    if text_blocks:
        this_outfile.write("\n\n".join(text_blocks) + "\n")
    if table_blocks:
        this_outfile.write("\n\n".join(table_blocks) + "\n")

    this_outfile.close()
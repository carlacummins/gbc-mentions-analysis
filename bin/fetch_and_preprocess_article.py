#!/usr/bin/env python3

"""
Fetch full text articles from Europe PMC by PMC ID(s), preprocess, and save as text files.
Preprocessing extracts text and tables from XML, cleans tags, and formats output.
Each article is saved as a single cleaned text file.
"""

import os
import argparse
from gbcutils.europepmc import get_fulltext_body
import gbcutils.europepmc as epmc

VERBOSE = False

parser = argparse.ArgumentParser(description="Fetch and preprocess articles.")
parser.add_argument('--pmcid', help='PMC ID of the article')
parser.add_argument('--idlist', help='Path to file containing list of PMC IDs')
parser.add_argument('--outdir', help='Directory to write output files', default='pmc_preprocessed')
parser.add_argument('--local_xml_dir', help='Directory containing local XML files', default=None)
parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
args = parser.parse_args()

VERBOSE = args.verbose
epmc.VERBOSE = VERBOSE

if not os.path.exists(args.outdir):
    os.makedirs(args.outdir)

local_xml_dir = args.local_xml_dir or os.path.join(args.outdir, "xml")

ids = []
if args.pmcid:
    ids = [args.pmcid]
elif args.idlist:
    with open(args.idlist, 'r') as f:
        ids = [line.strip() for line in f if line.strip()]
else:
    raise ValueError("You must provide either a PMC ID (--pmcid) or a file containing a list of PMC IDs (--idlist).")

for pmcid in ids:
    if VERBOSE:
        print(f"\n-- Processing {pmcid} --")
    this_outfile = open(f"{args.outdir}/{pmcid}.txt", 'w')
    text_blocks, table_blocks = get_fulltext_body(pmcid, dest=local_xml_dir) # fetch and parse the full text body
    if text_blocks:
        this_outfile.write("\n\n".join(text_blocks) + "\n")
    if table_blocks:
        this_outfile.write("\n\n".join(table_blocks) + "\n")

    this_outfile.close()

    # sometimes we get no data, so we remove empty files
    if os.path.getsize(this_outfile.name) == 0:
        os.remove(this_outfile.name)

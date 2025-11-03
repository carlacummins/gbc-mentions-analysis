#!/usr/bin/env python3

"""
Query Europe PMC searhch API for articles containing names/aliases of known biodata resources.
Store article metadata in sharded JSONL files and a SQLite database of PMC IDs. Multiple threads
are used to parallelize I/O-bound Europe PMC queries.

The resulting PMC ID list is deduplicated (using an SQLite database), sorted and split into chunks
for downstream processing.
"""


import json
import argparse
import os
import gzip
import sqlite3
import time
import math

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import threading
import queue
import traceback

from gbcutils.europepmc import epmc_search
from gbcutils.metadata import shard_key, shard_path

parser = argparse.ArgumentParser(description="Query Europe PMC for resource mentions.")
parser.add_argument('--outdir', type=str, required=True, help='Output directory for results')
parser.add_argument('--resources', type=str, required=True, help='JSON file containing resource names and aliases')
parser.add_argument('--chunks', type=int, default=1, help='Number of chunks to split the work into')
parser.add_argument('--epmc_limit', type=int, default=0, help='Limit for the number of results to fetch from Europe PMC')
parser.add_argument('--page_size', type=int, default=1000, help='Page size for Europe PMC queries (mostly for testing. default: 1000)')
parser.add_argument('--shards', type=int, default=128, help='Number of JSONL shards to write for metadata')
parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
parser.add_argument('--include_pmcids', type=str, default="", help='Comma-separated list of PMCIDs to include (for testing)')

parser.add_argument('--workers', type=int, default=4, help='Number of parallel threads for Europe PMC queries (I/O bound)')
parser.add_argument('--queue_size', type=int, default=1000, help='Max in-flight metadata records to buffer before writing')

args = parser.parse_args()

os.makedirs(args.outdir, exist_ok=True)
resource_aliases = json.load(open(args.resources))

metadata_outdir = os.path.join(args.outdir, "metadata")
os.makedirs(metadata_outdir, exist_ok=True)
ids_db_path = os.path.join(args.outdir, "pmc_ids.sqlite")

if args.verbose:
    import gbcutils.europepmc as epmc_utils
    epmc_utils.VERBOSE = True

extra_pmcids = set(x.strip() for x in args.include_pmcids.split(",") if x.strip())
if len(extra_pmcids) > 100:
    raise ValueError("You can only include up to 100 PMCIDs for testing.")

# -----------------------
# Sharded writer (single thread)
# -----------------------
writers = {}
writers_lock = threading.Lock()

work_q: queue.Queue = queue.Queue(maxsize=args.queue_size)


def _get_writer(k: int) -> gzip.GzipFile:
    """Get or create a shard writer for the given shard key."""
    with writers_lock:
        if k not in writers:
            writers[k] = gzip.open(shard_path(k, basepath=metadata_outdir, shards=args.shards), 'at', encoding='utf-8')
        return writers[k]


def _writer_thread_fn():
    """Thread function for writing metadata and PMC IDs."""
    try:
        conn = sqlite3.connect(ids_db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("CREATE TABLE IF NOT EXISTS pmc_ids (pmc_id TEXT PRIMARY KEY)")
        conn.commit()
        cur = conn.cursor()

        last_commit = time.time()
        while True:
            item = work_q.get()
            if item is None:
                work_q.task_done()
                break
            this_pmcid, this_article_metadata = item
            cur.execute("INSERT OR IGNORE INTO pmc_ids(pmc_id) VALUES (?)", (this_pmcid,))
            if cur.rowcount == 1:
                k = shard_key(this_pmcid, args.shards)
                fh = _get_writer(k)
                fh.write(json.dumps(this_article_metadata, ensure_ascii=False) + "\n")
            if time.time() - last_commit > 5:
                conn.commit()
                last_commit = time.time()
            work_q.task_done()
    except Exception:
        # write a crash log to disk so you see it even if stdout is buffered
        err_path = os.path.join(args.outdir, "writer.error.log")
        with open(err_path, "a", encoding="utf-8") as lf:
            lf.write(traceback.format_exc())
        raise
    finally:
        try:
            conn.commit()
            conn.close()
        except Exception:
            pass


writer_thread = threading.Thread(target=_writer_thread_fn, daemon=True)
writer_thread.start()

# -----------------------
# Query producers (thread pool)
# -----------------------

def build_resource_query(r_aliases: list[str]) -> str:
    """
    Build a query string for the given resource aliases.

    Args:
        r_aliases (list[str]): List of resource name aliases.

    Returns:
        str: Europe PMC query string.
    """
    ras = list(set(r_aliases))
    joined_aliases = " OR ".join(f'\"{alias}\"' for alias in ras if alias)
    return f"(HAS_FT:Y) AND ({joined_aliases})"

def build_pmcids_query(pmcids: list[str]) -> str:
    """
    Build a query string for the given PMC IDs.

    Args:
        pmcids (list[str]): List of PMC IDs.

    Returns:
        str: Europe PMC query string.
    """
    ids = list(set(pmcids))
    joined_ids = " OR ".join(f'PMCID:({pmcid})' for pmcid in ids if pmcid)
    return f"(HAS_FT:Y) AND ({joined_ids})"

epmc_fields = [
    "pmcid", "pmid", "title", "firstPublicationDate", "journalInfo", "authorString",
    "authorList", "citedByCount", "grantsList", "keywordList", "meshHeadingList"
]
BATCH_SIZE = 10_000
def produce_for_resource(r_aliases: list[str]) -> int:
    """
    Query Europe PMC API to produce metadata for articles matching the given resource aliases.

    Args:
        r_aliases (list[str]): List of resource name aliases.

    Returns:
        int: Number of articles produced.
    """
    epmc_query = build_resource_query(r_aliases)
    if args.verbose:
        print(f"Searching Europe PMC for: {epmc_query}")

    produced = 0
    cursor = None

    while True:
        # Respect an overall cap if provided
        if args.epmc_limit and produced >= args.epmc_limit:
            break
        # Ask for up to BATCH_SIZE per call; if epmc_limit is set and close, trim
        this_limit = BATCH_SIZE
        if args.epmc_limit:
            this_limit = min(this_limit, max(0, args.epmc_limit - produced))
            if this_limit == 0:
                break
        try:
            results, next_cursor = epmc_search(
                epmc_query,
                result_type='core',
                limit=this_limit,
                cursor=cursor,
                page_size=args.page_size, # mostly for testing
                returncursor=True,
                fields=epmc_fields,
            )
            if args.verbose:
                print(f"[progress] Europe PMC query for '{epmc_query}' returned {len(results)} results (cursor: {next_cursor})")
        except Exception as e:
            print(f"[WARNING] EuropePMC query failed: {e} :: {epmc_query}")
            break

        if not results:
            break

        for article in results:
            this_pmcid = article.get('pmcid')
            if not this_pmcid:
                continue
            # minimal metadata for downstream DB load (kept same fields for minimal change)
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
                'keywordList': article.get('keywordList', {}),
                'meshHeadingList': article.get('meshHeadingList', {}),
            }
            print(f"[debug] Producing metadata for {this_pmcid}") if args.verbose else None
            work_q.put((this_pmcid, this_article_metadata))
            produced += 1

        # Advance cursor; stop if EuropePMC indicates no further progress
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    return produced

def produce_for_ids(pmc_ids: list[str]) -> int:
    """
    Query Europe PMC API to produce metadata for articles matching the given PMC IDs.

    Args:
        pmc_ids (list[str]): List of PMC IDs.

    Returns:
        int: Number of articles produced.
    """
    epmc_query = build_pmcids_query(pmc_ids)
    if args.verbose:
        print(f"Searching Europe PMC for: {epmc_query}")

    produced = 0
    try:
        results = epmc_search(
            epmc_query,
            result_type='core',
            fields=epmc_fields,
        )
        if args.verbose:
            print(f"[progress] Europe PMC query for '{epmc_query}' returned {len(results)} results")
    except Exception as e:
        print(f"[WARNING] EuropePMC query failed: {e} :: {epmc_query}")
        return produced

    if not results:
        return produced

    for article in results:
        this_pmcid = article.get('pmcid')
        if not this_pmcid:
            continue
        # minimal metadata for downstream DB load (kept same fields for minimal change)
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
            'keywordList': article.get('keywordList', {}),
            'meshHeadingList': article.get('meshHeadingList', {}),
        }
        print(f"[debug] Producing metadata for {this_pmcid}") if args.verbose else None
        work_q.put((this_pmcid, this_article_metadata))
        produced += 1

    return produced


# Submit all resource queries in parallel
with ThreadPoolExecutor(max_workers=args.workers) as pool:
    futures = [pool.submit(produce_for_resource, r_aliases) for r_aliases in resource_aliases.values()]
    if extra_pmcids:
        # also submit a single batch query for any extra PMCIDs requested
        futures.append(pool.submit(produce_for_ids, list(extra_pmcids)))

    total_new = 0
    for fut in as_completed(futures):
        total_new += fut.result() or 0
        if args.verbose:
            print(f"[progress] total new PMCID metadata produced so far: {total_new}")

# Stop the writer thread
work_q.put(None)
work_q.join()
writer_thread.join()

# Close all shard writers
for fh in list(writers.values()):
    fh.close()


# Split the idlist into chunks by streaming IDs from SQLite (memory-light)
con = sqlite3.connect(ids_db_path)
cur = con.cursor()

# Count total IDs first (still memory-light)
cur.execute("SELECT COUNT(*) FROM pmc_ids")
(total_ids,) = cur.fetchone()
per_chunk = int(math.ceil(total_ids / args.chunks)) if args.chunks > 0 else total_ids

# Order by the numeric portion of the PMC ID to ensure true numeric order
cur.execute("SELECT pmc_id FROM pmc_ids ORDER BY CAST(SUBSTR(pmc_id, 4) AS INTEGER)")

chunk_idx = 0
written_in_chunk = 0
chunk_path = os.path.join(args.outdir, f"pmc_idlist.chunk_{chunk_idx+1}.txt")
cf = open(chunk_path, 'w')
try:
    total = 0
    for (pmc_id,) in cur:
        total += 1
        cf.write(pmc_id + "\n")
        written_in_chunk += 1
        # Rotate to the next chunk once we hit the target size, but leave any remainder in the last file
        if written_in_chunk >= per_chunk and chunk_idx < (args.chunks - 1):
            cf.close()
            chunk_idx += 1
            written_in_chunk = 0
            chunk_path = os.path.join(args.outdir, f"pmc_idlist.chunk_{chunk_idx+1}.txt")
            cf = open(chunk_path, 'w')
    if args.verbose:
        print(f"Saved {total} unique IDs into {chunk_idx+1} chunk files under {args.outdir} (≈{per_chunk} per chunk)")
finally:
    try:
        cf.close()
    except Exception:
        pass
    con.close()

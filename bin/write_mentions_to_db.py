#!/usr/bin/env python3

"""
For each resource mention classification in a CSV file, write the mentions to the database.

This includes creating Publication and ResourceMention objects in the GBC database using the
GlobalBioData Python API (https://globalbiodata.github.io/gbc-publication-analysis/), and
metadata from Europe PMC, saved in sharded JSONL files.
"""

import argparse
import json
import os
from pprint import pprint
import pandas as pd

from gbcutils.db import get_gbc_connection
from gbcutils.metadata import get_article_metadata, sort_ids_by_shard
import globalbiodata as gbc


parser = argparse.ArgumentParser()
parser.add_argument("--classifications", help="Path to CSV with mentions to write", required=True)
parser.add_argument("--metadata-dir", help="Base path to JSONLs with article metadata", required=True)
parser.add_argument("--shards", type=int, default=128, help="Number of JSONL shards used for metadata")
parser.add_argument("--resources", help="Path to JSON with resources and aliases", required=True)
parser.add_argument("--version-json", help="Path to JSON with version information", required=True)
parser.add_argument("--db-credentials", help="Path to DB credentials JSON")
parser.add_argument("--dry-run", action="store_true", help="If set, do not write to DB")
parser.add_argument("--test", action="store_true", help="Use test database instead of production")
parser.add_argument("--debug", action="store_true", help="Enable debug mode when writing to DB")
args = parser.parse_args()

# Load classifications data
classifications_df = pd.read_csv(args.classifications)
# Sort by shard grouping
sorted_ids = sort_ids_by_shard(classifications_df['id'], shards=args.shards)
classifications_df = classifications_df.set_index('id').loc[sorted_ids].reset_index()

print(f"[INFO] Loaded {len(classifications_df)} classifications from {args.classifications}")

# Load metadata
# articles_metadata = json.load(open(args.metadata))
# print(f"[INFO] Loaded metadata for {len(articles_metadata)} articles from {args.metadata}")

# Load resources metadata
resources_json = json.load(open(args.resources))
print(f"[INFO] Loaded {len(resources_json)} resources from {args.resources}")
resources_metadata = { # map names to ids : easy lookup for DB write
    resource_name: resource_id
    for resource_id, resource_names in resources_json.items()
    for resource_name in resource_names
}

# Create GCB version object
version_info = json.load(open(args.version_json))
gbc_version = gbc.Version(version_info)


# Connect to the database
# Load DB credentials
db_creds = json.load(open(args.db_credentials))
if not db_creds:
    raise ValueError("DB credentials are required for writing to the database.")

# Get DB connection
gcp_connector, db_engine, db_conn = get_gbc_connection(
    test=args.test,
    readonly=False,
    sqluser=db_creds['user'],
    sqlpass=db_creds['pass']
)

# Parse classifications and prepare data for DB insertion
try:
    previous_publication = None
    x, batch_size = 1, 1
    for row in classifications_df.itertuples(index=False):
        article_id = row.id
        resource = row.resource_name
        matched_alias = row.matched_alias
        match_count = int(row.match_count)
        mean_confidence = float(row.mean_confidence)

        # Prepare data for insertion
        article_metadata = get_article_metadata(article_id, basepath=args.metadata_dir, shards=args.shards)
        if not article_metadata:
            print(f"[WARNING] No metadata found for article ID {article_id}. Skipping.")
            continue
        if not article_metadata.get('title') or not article_metadata.get('authorList') or article_metadata.get('citedByCount') is None:
            print(f"[WARNING] Incomplete metadata for article ID {article_id}. Skipping.")
            continue

        resource_id = resources_metadata.get(resource)
        if not resource_id:
            print(f"[WARNING] No resource ID found for resource {resource}. Skipping.")
            continue

        mentions_data = {
            "publication": f"{article_id} : {article_metadata.get('title', 'Unknown Title')}",
            "resource": gbc.Resource({"id": resource_id, "short_name": resource}),
            "version": gbc_version,
            "matched_alias": matched_alias,
            "match_count": match_count,
            "mean_confidence": mean_confidence,
        }

        if args.dry_run:
            print(f"[DRY RUN] Would insert: \n{mentions_data.__str__()}")
        else:
            # Insert into the database
            if previous_publication and previous_publication.pmc_id == article_metadata['pmcid']:
                # If the publication is the same as the previous one, reuse it
                gbc_publication = previous_publication
            else:
                # create new publication
                print("[INFO] Creating new Publication from EuropePMC result... ") if args.debug else None
                pprint(article_metadata) if args.debug else None
                gbc_publication = gbc.new_publication_from_EuropePMC_result(article_metadata, google_maps_api_key=os.environ.get('GOOGLE_MAPS_API_KEY'))
                # gbc_publication.write(conn=db_conn, debug=args.debug)
                gbc_publication.write(engine=db_engine, debug=args.debug)

            print("[INFO] working with publication: ", gbc_publication) if args.debug else None
            mentions_data["publication"] = gbc_publication

            gbc_mention = gbc.ResourceMention(mentions_data)
            gbc_mention.write(conn=db_conn, debug=args.debug)
            previous_publication = gbc_publication

            if x % batch_size == 0:
                print("📥 Committing transaction...") if args.debug else None
                db_conn.commit()
            x += 1

finally:
    # Clean shutdown
    try:
        print("📥 Committing transaction...") if args.debug else None
        db_conn.commit()
        db_conn.close()
    except Exception:
        pass
    try:
        db_engine.dispose()
    except Exception:
        pass
    try:
        gcp_connector.close()
    except Exception:
        pass
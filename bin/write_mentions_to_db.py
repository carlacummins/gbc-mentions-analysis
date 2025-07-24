#!/usr/bin/env python3
import argparse
import json
import os
import pandas as pd

from utils.gbc_db import get_gbc_connection
import globalbiodata as gbc


parser = argparse.ArgumentParser()
parser.add_argument("--classifications", help="Path to CSV with mentions to write", required=True)
parser.add_argument("--metadata", help="Path to JSON with metadata for articles", required=True)
parser.add_argument("--resources", help="Path to JSON with resources and aliases", required=True)
parser.add_argument("--version-json", help="Path to JSON with version information", required=True)
parser.add_argument("--db-credentials", help="Path to DB credentials JSON")
parser.add_argument("--dry-run", action="store_true", help="If set, do not write to DB")
parser.add_argument("--test", action="store_true", help="Use test database instead of production")
parser.add_argument("--debug", action="store_true", help="Enable debug mode when writing to DB")
args = parser.parse_args()

# Load classifications data
classifications_df = pd.read_csv(args.classifications)
print(f"[INFO] Loaded {len(classifications_df)} rows from {args.classifications}")

# Load metadata
articles_metadata = json.load(open(args.metadata))
print(f"[INFO] Loaded metadata for {len(articles_metadata)} articles from {args.metadata}")

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

# Parse classifications and prepare data for DB insertion
for _, row in classifications_df.iterrows():
    article_id = row['id']
    resource = row['resource_name']
    matched_alias = row['matched_alias']
    match_count = row['match_count']
    mean_confidence = row['mean_confidence']

    # Prepare data for insertion
    article_metadata = articles_metadata.get(article_id, {})
    if not article_metadata:
        print(f"[WARNING] No metadata found for article ID {article_id}. Skipping.")
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
        # Load DB credentials
        db_creds = json.load(open(args.db_credentials))
        if not db_creds:
            raise ValueError("DB credentials are required for writing to the database.")

        # Get DB connection
        gcp_connector, db_engine, connection = get_gbc_connection(
            test=args.test,
            readonly=False,
            sqluser=db_creds['user'],
            sqlpass=db_creds['pass']
        )

        # Insert into the database
        gbc_publication = gbc.new_publication_from_EuropePMC_result(article_metadata, google_maps_api_key=os.environ.get('GOOGLE_MAPS_API_KEY'))
        mentions_data["publication"] = gbc_publication

        gbc_mention = gbc.ResourceMention(mentions_data)
        gbc_mention.write(engine=db_engine, debug=args.debug)
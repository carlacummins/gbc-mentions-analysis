#!/usr/bin/env python3

import json
import argparse
import sqlalchemy as db
from utils.gbc_db import get_gbc_connection

parser = argparse.ArgumentParser(description="Fetch resource list from the database.")
parser.add_argument('--out', type=str, required=True, help='Output file for results')
parser.add_argument(
    '--sql', type=str, help='Search query for fetching resources from GBC database (optional)',
    default='SELECT short_name, common_name, full_name FROM resource WHERE is_latest = 1'
)
parser.add_argument("--aliases", type=str, help="JSON file containing any additional aliases for resources (optional)")
args = parser.parse_args()

gcp_connector, db_engine, db_conn = get_gbc_connection(test=False, readonly=True)


additional_aliases = json.load(open(args.aliases)) if args.aliases else {}

sql = args.sql
result = db_conn.execute(db.text(sql)).fetchall()
resource_names = []
for r in result:
    short_name = r[0].strip()
    common_name = r[1].strip() if r[1] else None
    full_name = r[2].strip() if r[2] else None
    if short_name:
        resource_names.append([short_name])
    if common_name and common_name != short_name:
        resource_names[-1].append(common_name)
    if full_name and full_name != short_name and full_name != common_name:
        resource_names[-1].append(full_name)

    if short_name in additional_aliases:
        resource_names[-1].extend(additional_aliases[short_name])

json.dump(resource_names, open(args.out, 'w'), indent=2)
print(f"📥 Saved {len(resource_names)} resources to {args.out}")
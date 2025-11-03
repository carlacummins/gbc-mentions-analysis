#!/usr/bin/env python3

"""
Fetch resource list from the database and save to a JSON file.
Optionally include additional aliases for resources from a provided JSON file.

This will be used to load resource names and aliases for mention detection and classification.
"""

import json
import argparse
import sqlalchemy as db
from gbcutils.db import get_gbc_connection

parser = argparse.ArgumentParser(description="Fetch resource list from the database.")
parser.add_argument('--out', type=str, required=True, help='Output file for results')
parser.add_argument("--aliases", type=str, help="JSON file containing any additional aliases for resources (optional)")
parser.add_argument("--test", action="store_true", help="Use test database instead of production")
parser.add_argument("--limit", type=int, help="Limit the number of resources fetched")
args = parser.parse_args()

gcp_connector, db_engine, db_conn = get_gbc_connection(test=args.test, readonly=True)


additional_aliases = json.load(open(args.aliases)) if args.aliases else {}

sql = 'SELECT id, short_name, common_name, full_name FROM resource WHERE is_latest = 1'
if args.limit:
    sql += f' LIMIT {args.limit}'

result = db_conn.execute(db.text(sql)).fetchall()
resource_names = {}
for r in result:
    rid = r[0]
    short_name = r[1].strip()
    common_name = r[2].strip() if r[2] else None
    full_name = r[3].strip() if r[3] else None

    resource_names[rid] = []
    if short_name:
        resource_names[rid].append(short_name)
    if common_name and common_name != short_name:
        resource_names[rid].append(common_name)
    if full_name and full_name != short_name and full_name != common_name:
        resource_names[rid].append(full_name)

    if short_name in additional_aliases:
        resource_names[rid].extend(additional_aliases[short_name])


json.dump(resource_names, open(args.out, 'w'), indent=2)
print(f"📥 Saved {len(resource_names)} resources to {args.out}")
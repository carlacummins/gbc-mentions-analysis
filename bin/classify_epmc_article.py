#!/usr/bin/env python3

import os
import json
import argparse

import sqlalchemy as db
from nltk.tokenize import sent_tokenize
import pandas as pd

from utils.europepmc import query_europepmc, get_fulltext_body
from utils.scibert_classify import get_resource_mentions, classify_mentions, load_model

parser = argparse.ArgumentParser(description="Classify resource mentions in a publication.")
parser.add_argument("-pmid", type=str, default="", help="PubMed ID of the publication")
parser.add_argument("-pmcid", type=str, default="", help="PMC ID of the publication")
parser.add_argument("-json", type=str, default="", help="JSON file containing PubMed IDs or PMC IDs (optional)")
parser.add_argument("-model", type=str, default="../data/models/scibert_resource_classifier.v2", required=True, help="Path to the SciBERT model")
parser.add_argument("-aliases", type=str, help="JSON file containing any additional aliases for resources (optional)")
parser.add_argument("-outdir", type=str, help="Output directory for results", required=True)
args = parser.parse_args()

pmid = args.pmid
pmcid = args.pmcid
json_file = args.json
model_path = args.model
resource_aliases_path = args.aliases
output_dir = args.outdir
os.makedirs(output_dir, exist_ok=True)

if not pmid and not pmcid:
    raise ValueError("You must provide either a PubMed ID (-pmid) or a PMC ID (-pmcid).")

epmc_base_url = "https://www.ebi.ac.uk/europepmc/webservices/rest"


"""# 📥 Pull resource list from DB"""
print("📥 Pulling resource list from DB")

# @title 🔎 DB connection setup (via public IP)
db_engine = db.create_engine('mysql+pymysql://gbcreader@34.89.127.34/gbc-publication-analysis', pool_recycle=3600, pool_size=50, max_overflow=50)
db_conn = db_engine.connect()

print("\t🔗 Successfully connected to GBC MySQL instance")

# @title 📋 Load resource list

additional_aliases = json.load(open(resource_aliases_path))

sql = "SELECT short_name, common_name, full_name FROM resource WHERE is_latest=1"
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

print(f"\t📥 Loaded {len(resource_names)} resources")

"""
📦 Load Model
"""
(tokenizer, model, device) = load_model(model_path)

"""
🧠 Begin Predictions
"""

# fetch publication metadata from Europe PMC
if pmid or pmcid:
    epmc_query = f"PMCID:{pmcid}" if pmcid else f"EXT_ID:{pmid}"
    print(f"\t‣ 🔍 Querying Europe PMC for {epmc_query}")
    data = query_europepmc(f"{epmc_base_url}/search", request_params={
        'query': epmc_query,
        'format': 'json',
        'pageSize': 10,
        'cursorMark': '*',
        'resultType': 'core'
    })
    results = data.get('resultList', {}).get('result', [])
    print(f"\t\t‣ 🔍 Found {data.get('hitCount', 0)} results for {epmc_query}")
    print("\n")
else:
    # If no pmid or pmcid is provided, load from JSON file
    if not json_file:
        raise ValueError("You must provide a JSON file containing PubMed IDs or PMC IDs.")
    with open(json_file, 'r') as f:
        results = list(json.load(f).values())


# Identify potential matches in the results and aggregate
mentions = []
for result in results:
    this_pmcid = result.get('pmcid')
    this_pmid = result.get('pmid')
    title = result.get('title')

    # since we must use EXT_ID to search using PMID, this introduces room for error
    # keep skipping through results until the match is found.
    # In theory, we should only be processing 1 publication here.
    if pmid and this_pmid != pmid:
        continue

    if pmcid:
        # Get full text body and tables
        text_body, table_blocks = get_fulltext_body(this_pmcid)
        if not text_body:
            print("\t⚠️ No full text body found.")
            continue
    else:
        text_body, table_blocks = sent_tokenize(result.get('abstractText')), []

    if pmcid or pmid:
        break # only use the first successful match

    text_body = [tb.replace('\n', ' ') for tb in text_body]
    text_body = [tb for tb in text_body if tb.strip()]
    print(f"\t\t\t‣ Processed {len(text_body)} text blocks")
    print(f"\t\t\t‣ Processed {len(table_blocks)} table blocks")
    print("\n\n")

    print(f"🧠 Identifying resource mentions in {this_pmcid}...")
    # @title 🔍 Search for resource mentions
    print(f"\t‣ 🔍 Searching for resource mentions in {this_pmcid}...")
    this_article_mentions = get_resource_mentions(text_body, table_blocks, resource_names)
    print(f"\t‣ 🔍 Found {len(this_article_mentions)} mentions of {len(set([x[2] for x in this_article_mentions]))} resources in {this_pmcid}.")

    mentions.extend(this_article_mentions)

"""
🧠 Classify resource mentions
"""
classified_mentions = classify_mentions(this_pmcid, this_pmid, mentions, tokenizer=tokenizer, model=model, device=device)
class_df = pd.DataFrame(classified_mentions)
class_df.sort_values(by=['prediction', 'confidence'], ascending=[False, False], inplace=True)

"""
🏁 Publication Classification Final Result
"""

# save CSV file with all 'true' mentions
summary_df = (
    class_df[(class_df['prediction'] == 1) & (class_df['confidence'] >= 0.9)]
    .groupby(['pmcid', 'pmid', 'resource_name', 'matched_alias'], as_index=False)
    .agg({
        'confidence': 'mean',
        'prediction': 'count',
        'sentence': lambda x: " || ".join(list(set(x)))  # unique sentences
    })
)
summary_df.rename(columns={'confidence': 'mean_confidence', 'sentence':'token_matches', 'prediction': 'num_matches'}, inplace=True)
summary_df[['pmcid', 'pmid', 'resource_name', 'matched_alias', 'num_matches', 'mean_confidence']].to_csv(f"{output_dir}/resource_mentions_summary.csv", index=False)

# save classification result counts for each resource
specificity_df = (
    class_df
    .groupby(['resource_name', 'prediction'], as_index=False)
    .agg(count=('prediction', 'count'))
)
specificity_df.to_pickle(f"{output_dir}/prediction_counts.pkl")
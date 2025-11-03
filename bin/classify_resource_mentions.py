#!/usr/bin/env python3

"""
Given a text file or directory of text files containing publication text, identify resource mentions,
classify them using a SciBERT model, and save the results to output files.
"""

import os
import json
import argparse
import glob

import pandas as pd
from gbcutils.scibert_classify import get_resource_mentions, classify_mentions, load_model
import gbcutils.scibert_classify as utils

parser = argparse.ArgumentParser(description="Classify resource mentions in a publication.")
parser.add_argument("--txt", type=str, default=None, help="Text file containing publication text")
parser.add_argument("--indir", type=str, default=None, help="Input directory containing text files (optional)")

parser.add_argument("--model", type=str, default="../data/models/scibert_resource_classifier.v2", required=True, help="Path to the SciBERT model")
parser.add_argument("--resources", type=str, required=True, help="JSON file containing resources names and aliases")
parser.add_argument("--case_sensitive_resources", type=str, default="", help="Comma-separated list of resources to search case-sensitively")
parser.add_argument("--mentions_out", type=str, default="resource_mentions_summary.csv", help="Output file for resource mentions")
parser.add_argument("--counts_out", type=str, default="prediction_counts.pkl", help="Output file for prediction counts")
parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
args = parser.parse_args()

model_path = args.model
resource_aliases_path = args.resources

VERBOSE = args.verbose
utils.VERBOSE = VERBOSE

case_sensitive_resources = [x.strip() for x in args.case_sensitive_resources.split(',') if x.strip()]

if not args.txt and not args.indir:
    raise ValueError("You must provide either a text file (--txt) or an input directory (--indir).")
filelist = glob.glob(os.path.join(args.indir, "*.txt")) if args.indir else [args.txt]


# 📋 Load resource list
resource_names = json.load(open(resource_aliases_path))
resource_names = resource_names.values()
print(f"\t📥 Loaded {len(resource_names)} resources") if args.verbose else None

# 📦 Load Model
print("📦 Loading SciBERT resource classifier model") if args.verbose else None
(tokenizer, model, device) = load_model(model_path)

# 🧠 Run Predictions
print("🧠 Running predictions") if args.verbose else None
class_df = pd.DataFrame(columns=['prediction', 'id', 'resource_name', 'matched_alias', 'sentence', 'confidence'])
for txt_file in filelist:
    text_body = open(txt_file, 'r').read()
    mentions = get_resource_mentions(text_body, resource_names, case_sensitive_resources=case_sensitive_resources)
    print(f"\t‣ 🔍 Found {len(mentions)} mentions of {len(set([x[2] for x in mentions]))} resources in {txt_file}.") if args.verbose else None
    if not mentions:
        print(f"\t‣ ❌ No resource mentions found in {txt_file}. Skipping classification.") if args.verbose else None
        continue

    # @title 🧠 Classify resource mentions
    this_id = os.path.basename(txt_file).replace('.txt', '')
    classified_mentions = classify_mentions(this_id, mentions, tokenizer=tokenizer, model=model, device=device)
    this_class_df = pd.DataFrame(classified_mentions)
    this_class_df.sort_values(by=['prediction', 'confidence'], ascending=[False, False], inplace=True)
    class_df = pd.concat([class_df, this_class_df], ignore_index=True)


"""## 🏁 Publication Classification Final Result"""
if args.verbose:
    print(f"🏁 Publication Classification Final Result for {len(filelist)} files")
    print(f"\t‣ Found {len(class_df)} classified mentions across {len(set(class_df['id']))} publications.")

summary_df = (
    class_df[(class_df['prediction'] == 1) & (class_df['confidence'] >= 0.9)]
    .groupby(['id', 'resource_name', 'matched_alias'], as_index=False)
    .agg({
        'confidence': 'mean',
        'prediction': 'count',
        'sentence': lambda x: " || ".join(list(set(x)))  # unique sentences
    })
)
summary_df.rename(columns={'confidence': 'mean_confidence', 'sentence':'token_matches', 'prediction': 'match_count'}, inplace=True)

# write files
summary_df[['id', 'resource_name', 'matched_alias', 'match_count', 'mean_confidence']].to_csv(f"{args.mentions_out}", index=False)

specificity_df = (
    class_df
    .groupby(['resource_name', 'matched_alias', 'prediction'], as_index=False)
    .agg(count=('prediction', 'count'))
)
specificity_df.to_pickle(f"{args.counts_out}")
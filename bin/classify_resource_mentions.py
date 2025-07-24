#!/usr/bin/env python3

import os
import json
import argparse
import glob

import pandas as pd
from tabulate import tabulate

from utils.scibert_classify import get_resource_mentions, classify_mentions, load_model

parser = argparse.ArgumentParser(description="Classify resource mentions in a publication.")
parser.add_argument("--txt", type=str, required=True, help="Text file containing publication text")
parser.add_argument("--indir", type=str, default=None, help="Input directory containing text files (optional)")

parser.add_argument("--model", type=str, default="../data/models/scibert_resource_classifier.v2", required=True, help="Path to the SciBERT model")
parser.add_argument("--resources", type=str, required=True, help="JSON file containing resources names and aliases")

parser.add_argument("--mentions_out", type=str, default="resource_mentions_summary.csv", help="Output file for resource mentions")
parser.add_argument("--counts_out", type=str, default="prediction_counts.pkl", help="Output file for prediction counts")
args = parser.parse_args()

model_path = args.model
resource_aliases_path = args.resources

if not args.txt and not args.indir:
    raise ValueError("You must provide either a text file (--txt) or an input directory (--indir).")
filelist = glob.glob(os.path.join(args.indir, "*.txt")) if args.indir else [args.txt]


# 📋 Load resource list
resource_names = json.load(open(resource_aliases_path))
resource_names = resource_names.values()
print(f"\t📥 Loaded {len(resource_names)} resources")

# 📦 Load Model
print("📦 Loading SciBERT resource classifier model")
(tokenizer, model, device) = load_model(model_path)

# 🧠 Run Predictions
print("🧠 Running predictions")
class_df = pd.DataFrame(columns=['prediction', 'id', 'resource_name', 'matched_alias', 'sentence', 'confidence'])
for txt_file in filelist:
    text_body = open(txt_file, 'r').read()
    mentions = get_resource_mentions(text_body, resource_names)
    print(f"\t‣ 🔍 Found {len(mentions)} mentions of {len(set([x[2] for x in mentions]))} resources in {txt_file}.")

    # @title 🧠 Classify resource mentions
    this_id = os.path.basename(txt_file).replace('.txt', '')
    classified_mentions = classify_mentions(this_id, mentions, tokenizer=tokenizer, model=model, device=device)
    this_class_df = pd.DataFrame(classified_mentions)
    this_class_df.sort_values(by=['prediction', 'confidence'], ascending=[False, False], inplace=True)
    class_df = pd.concat([class_df, this_class_df], ignore_index=True)
    print("\n\n")


"""## 🏁 Publication Classification Final Result"""
print("🏁 Publication Classification Final Result 🏁")

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

if len(summary_df) > 0:
    print(tabulate([[" \n ✅ Text _has_ verified known biodata resource mention(s)\n "]], tablefmt='grid'))
    print("\n\n* 🧩 Resource Match Summary *")
    print(tabulate(summary_df[['id', 'resource_name', 'matched_alias', 'match_count', 'mean_confidence']], headers='keys', tablefmt='grid', showindex=False))

else:
    print("\t‣ ❌ Text _does not_ mention a known biodata resource")
summary_df[['id', 'resource_name', 'matched_alias', 'match_count', 'mean_confidence']].to_csv(f"{args.mentions_out}", index=False)

specificity_df = (
    class_df
    .groupby(['resource_name', 'prediction'], as_index=False)
    .agg(count=('prediction', 'count'))
)
specificity_df.to_pickle(f"{args.counts_out}")
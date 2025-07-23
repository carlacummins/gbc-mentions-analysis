
#!/usr/bin/env python3
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description="Format CSV data for SciBERT input.")
parser.add_argument("--input", "-i", type=str, required=True, help="Path to input CSV file")
parser.add_argument("--output", "-o", type=str, required=True, help="Path to output CSV file")
args = parser.parse_args()

# Load your CSV file
input_csv = args.input
output_csv = args.output

df = pd.read_csv(input_csv)

# Create a new column combining text and matched_term for BERT input
df["input_text"] = df["paragraph_text"].astype(str) + " [SEP] " + df["matched_term"].astype(str)

# Keep only what's needed
df[["input_text", "label", "resource_name", "matched_term"]].to_csv(output_csv, index=False)

print(f"✅ Saved formatted data to {output_csv}")

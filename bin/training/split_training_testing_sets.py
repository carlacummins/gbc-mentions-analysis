#!/usr/bin/env python3

import pandas as pd

# Config
input_csv = "training_set_sentences.full.csv"
train_csv = "train_split.csv"
test_csv = "test_split.csv"
train_size = 1200  # number of training examples

# Load and shuffle
df = pd.read_csv(input_csv)
df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)

# Split
train_df = df_shuffled.iloc[:train_size]
test_df = df_shuffled.iloc[train_size:]

# Save
train_df.to_csv(train_csv, index=False)
test_df.to_csv(test_csv, index=False)

print(f"✅ Saved {len(train_df)} rows to {train_csv}")
print(f"✅ Saved {len(test_df)} rows to {test_csv}")
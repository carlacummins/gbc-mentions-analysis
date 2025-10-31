#!/usr/bin/env python3

import sys
import pandas as pd

pickle_files = sys.argv[1:]

# load and concatenate
dfs = [pd.read_pickle(f) for f in pickle_files]
merged = pd.concat(dfs, ignore_index=True)

# group by resource_name and prediction to sum counts
all_prediction_counts = (
    merged
    .groupby(["resource_name", "matched_alias", "prediction"], as_index=False)['count']
    .sum()
)

# Pivot prediction into columns
specificity_score = all_prediction_counts.pivot_table(
    index=['resource_name', 'matched_alias'],
    columns='prediction',
    values='count',
    fill_value=0
).reset_index()
specificity_score.columns.name = None  # Remove the name of the columns index

# Rename columns for clarity
specificity_score.rename(columns={
    1: 'positives',
    0: 'negatives'
}, inplace=True)

# Calculate specificity score : proportion of positive predictions
specificity_score['specificity'] = specificity_score['positives'] / (specificity_score['positives'] + specificity_score['negatives'])

# Save the specificity score DataFrame to a CSV file
specificity_score['positives'] = specificity_score['positives'].astype(int)
specificity_score['negatives'] = specificity_score['negatives'].astype(int)
specificity_score.sort_values(
    by=['specificity', 'positives'],
    ascending=[False, False],
    inplace=True
)
specificity_score.to_csv('resource_specificity_scores.csv', index=False)
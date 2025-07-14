#!/usr/bin/env python3
import csv
import sys
import os

input_file = sys.argv[1]

# os.system(f"cp {input_file} {input_file}.bkup")
with open(input_file, newline='', encoding='utf-8') as csvfile:
    reader = list(csv.DictReader(csvfile))
    # Replace 'label' and 'your_second_header' with your actual header names
    sorted_rows = sorted(reader, key=lambda row: (row['label'], row['article_topic_match'], row['pmcid'], row['resource_name'], row['matched_alias'] ))

with open(f"{input_file}.sorted", 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=reader[0].keys())
    writer.writeheader()
    writer.writerows(sorted_rows)
os.system(f"mv {input_file}.sorted {input_file}")
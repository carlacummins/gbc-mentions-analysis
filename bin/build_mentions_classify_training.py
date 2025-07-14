#!/usr/bin/env python3

import sys
import json
import requests
import time
import csv

from bs4 import BeautifulSoup
from nltk.tokenize import sent_tokenize
from tqdm import tqdm
import re
import random

# ---- Config ----
RESOURCE_FILE = sys.argv[1]
# OUTPUT_CSV = "training_set_sentences.csv"
MAX_ARTICLES_PER_ALIAS = 40
MAX_SENTENCES_PER_ARTICLE = 5  # Limit to 5 sentences per article to avoid too much data
POSITIVE_NEGATIVE_RATIO = 1  # target 1:1
EXP_POS = int(MAX_ARTICLES_PER_ALIAS/(POSITIVE_NEGATIVE_RATIO+1))
EXP_NEG = int(MAX_ARTICLES_PER_ALIAS - EXP_POS)

# ---- Load resource definitions ----
with open(RESOURCE_FILE, "r", encoding="utf-8") as f:
    resources = json.load(f)

# ---- Helper functions ----

def europepmc_query(name, page_size=1000, cursor_mark="*"):
    query = f'"{name}" AND OPEN_ACCESS:Y'
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/search?query={query}&format=json&pageSize={page_size}&cursorMark={cursor_mark}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def fetch_fulltext_blocks(pmcid):
    """Extracts sentences from paragraphs and table rows from full-text XML."""
    url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid}/fullTextXML"
    response = requests.get(url)
    if not response.ok:
        return []

    soup = BeautifulSoup(response.text, "lxml-xml")
    blocks = []

    # Extract paragraph sentences
    for p in soup.find_all("p"):
        text = p.get_text(" ", strip=True)
        if text:
            sentences = sent_tokenize(text)
            blocks.extend(sentences)

    # Extract tables
    for table_wrap in soup.find_all("table-wrap"):
        caption = table_wrap.find("caption")
        if caption:
            cap_text = caption.get_text(" ", strip=True)
            if cap_text:
                blocks.append(f"[TABLE-CAPTION] {cap_text}")

        table = table_wrap.find("table")
        if table:
            for row in table.find_all("tr"):
                cells = row.find_all(["td", "th"])
                row_text = " ".join(cell.get_text(" ", strip=True) for cell in cells if cell.get_text(strip=True))
                if row_text:
                    blocks.append(row_text)

    return blocks

def paragraph_label(text, alias, topics):
    lower = text.lower()
    match_name = alias.lower() in lower
    match_topic = any(
        re.search(r'\b' + re.escape(t.lower()) + r'e?s?\b', lower)
        for t in topics
    ) if topics else True
    if match_name and match_topic:
        return 1
    elif match_name:
        return 0
    return None

def print_progress(searched, total, pos=0, neg=0, bar_length=200):
    if searched <= 1:
        sys.stdout.write("\n\n\n\n\n")  # Reserve lines for output

    percent = "{0:.1f}".format(100 * (searched / float(total)))
    filled_length = int(bar_length * searched // total)
    bar = '=' * filled_length + ' ' * (bar_length - filled_length)

    sys.stdout.write("\033[5A")  # Move up 5 lines
    sys.stdout.write(f"\033[KSearching: |{bar}| ({searched:,}/{total:,}) {percent}%\n")
    sys.stdout.write("\033[K\n")  # Spacer line
    sys.stdout.write(f"\033[KPositives: |{'=' * pos}{" "*(EXP_POS-pos)}| ({pos}/{EXP_POS})\n")
    sys.stdout.write(f"\033[KNegatives: |{'=' * neg}{" "*(EXP_NEG-neg)}| ({neg}/{EXP_NEG})\n")
    sys.stdout.write("\033[K\n")  # Spacer line

    sys.stdout.flush()


# ---- Main processing loop ----
# results = []
skip_after = 200  # Skip resources after this many articles if no negatives found

for resource in tqdm(resources, desc="Resources"):
    aliases = resource["names"]
    topics = resource.get("topics", [])
    if not topics or resource.get("ambiguous", 1) == 0:
        # Skip resources without topics or ambiguous ones
        print(f"Skipping resource '{aliases[0]}' - {"no topics defined" if not topics else "non-ambiguous resource name"}.")
        continue


    resource_examples = []
    fetched_resource = 0
    total_pos, total_neg = 0, 0

    for alias in aliases:
        alias_examples = []
        print(f"\nProcessing resource '{aliases[0]}' with alias '{alias}'...")
        skip_alias = False
        cursor = "*"
        fetched = 0
        pos, neg = 0, 0


        # while fetched < MAX_ARTICLES_PER_ALIAS and (neg == 0 or (pos / (neg or 1)) < POSITIVE_NEGATIVE_RATIO):
        while pos < EXP_POS or neg < EXP_NEG:
            try:
                data = europepmc_query(alias, cursor_mark=cursor)
                if cursor == "*":
                    results_for_alias = data.get("hitCount", 0)
                    print(f"🔎 Found {results_for_alias:,} Europe PMC for alias '{alias}'...")
                    skip_after = min(skip_after, results_for_alias)
                articles = data.get("resultList", {}).get("result", [])
                random.shuffle(articles)
                cursor = data.get("nextCursorMark", "*")
                if not articles:
                    break
            except Exception as e:
                print(f"Query failed for alias {alias}: {e}")
                break

            for article in articles:
                sentences_from_article = 0
                pmcid = article.get("pmcid")
                if not pmcid:
                    continue

                try:
                    text_blocks = fetch_fulltext_blocks(pmcid)

                    # search whole article for topics
                    article_topic_match = any(t.lower() in ';'.join(text_blocks).lower() for t in topics)

                    for block in text_blocks:
                        label = paragraph_label(block, alias, topics)
                        if label is None:
                            continue
                        if label == 1:
                            if pos >= EXP_POS:
                                continue
                            else:
                                pos += 1
                        elif label == 0:
                            if neg >= EXP_NEG:
                                continue
                            else:
                                neg += 1

                        alias_examples.append({
                            "resource_name": aliases[0],
                            "matched_alias": alias,
                            "label": label,
                            "article_topic_match": article_topic_match,
                            "pmcid": pmcid,
                            "paragraph_text": block.replace("\n", " ").strip()
                        })
                        sentences_from_article += 1
                        if sentences_from_article >= MAX_SENTENCES_PER_ARTICLE:
                            break


                    time.sleep(0.2)
                except Exception as e:
                    print(f"Error processing PMC{pmcid}: {e}")
                    continue

                fetched += 1
                fetched_resource += 1
                print_progress(fetched, results_for_alias, pos, neg)
                if fetched >= skip_after and (neg == 0 or pos == 0):
                    print(f"⚠️ Skipping alias '{alias}' — no {"negatives" if neg == 0 else "positives"} found in first {skip_after} articles.")
                    skip_alias = True
                    break

                if pos >= EXP_POS and neg >= EXP_NEG:
                    print(f"✅ Found enough examples for alias '{alias}': {pos} positives, {neg} negatives.")
                    break

                # if neg > 0 and pos / neg >= POSITIVE_NEGATIVE_RATIO:
                #     break

            if skip_alias:
                break
            else:
                resource_examples.extend(alias_examples)
                print("Completed fetching articles for alias:", alias)

        total_pos += pos
        total_neg += neg

    # results.extend(resource_examples)

    # ---- Write to CSV ----
    outfile = f"training_set_sentences/training_set_sentences.{aliases[0]}.csv"
    with open(outfile, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["resource_name", "matched_alias", "label", "article_topic_match", "pmcid", "paragraph_text"])
        writer.writeheader()
        for row in resource_examples:
            writer.writerow(row)

    print(f"\n✅ Saved {len(resource_examples)} examples to {outfile} for resource '{aliases[0]}' with {total_pos} positives and {total_neg} negatives.")

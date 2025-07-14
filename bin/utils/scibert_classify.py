#!/usr/bin/env python3

import re
from collections import Counter
from tqdm import tqdm
import torch
from nltk.tokenize import sent_tokenize
from transformers import AutoTokenizer, AutoModelForSequenceClassification

def _remove_substring_matches(mentions):
    aliases = [m[1].lower() for m in mentions]
    unique_aliases = list(set(aliases))

    substr_aliases = []
    for alias1 in unique_aliases:
        for alias2 in unique_aliases:
            if alias1 in alias2 and alias1 != alias2:
                substr_aliases.append(alias1)

    for alias in substr_aliases:
        mentions = [m for m in mentions if m[1].lower() != alias]

    return mentions

case_sensitive_threshold = 30 # switch to case sensitive search after this number of matches for a resource
def get_resource_mentions(textblocks, tableblocks, resource_names):
    mentions = []

    # precompile regex patterns for each resource alias
    # This is more efficient than compiling them on-the-fly in the loop
    compiled_patterns = []
    for resource in resource_names:
        resource_name = resource[0]
        for alias in resource:
            pattern_case_insensitive = re.compile(rf"[^A-Za-z]{re.escape(alias.lower())}[^A-Za-z]")
            compiled_patterns.append((resource_name, alias, pattern_case_insensitive))

    # Split the fulltext into sentences and table rows
    for block in textblocks:
        # sentences = block.split('. ')
        sentences = sent_tokenize(block)  # Use NLTK to split into sentences
        for sentence in sentences:
            sentence = sentence.replace("\n", " ")
            s_lowered = sentence.lower()
            this_sentence_mentions = []
            for resource_name, alias, pattern_ci in compiled_patterns:
                if pattern_ci.search(s_lowered):
                    this_sentence_mentions.append((sentence.strip(), alias, resource_name))

            if len(this_sentence_mentions) > 1:
                this_sentence_mentions = _remove_substring_matches(this_sentence_mentions)
            mentions.extend(this_sentence_mentions)

    for table in tableblocks:
        rows = table.split('\n')

        for row in rows:
            r_lowered = row.lower()
            this_row_mentions = []
            for resource_name, alias, pattern_ci in compiled_patterns:
                if pattern_ci.search(r_lowered):
                    this_row_mentions.append((row.strip(), alias, resource_name))

            if len(this_row_mentions) > 1:
                this_row_mentions = _remove_substring_matches(this_row_mentions)
            mentions.extend(this_row_mentions)

    # if a large number of matches are found for one resource, switch to case sensitive mode
    filtered_mentions = []
    alias_counts = Counter([m[1] for m in mentions])
    for alias, count in alias_counts.items():
        if count > case_sensitive_threshold:
            print(f"⚠️ {count} matches found for {alias} - switching to case sensitive mode")
            pattern_case_sensitive = re.compile(rf"[^A-Za-z]{re.escape(alias)}[^A-Za-z]")
            for m in mentions:
                if m[1] == alias and pattern_case_sensitive.search(m[0]):
                    filtered_mentions.append(m)
        else:
            this_alias_mentions = [m for m in mentions if m[1] == alias]
            filtered_mentions.extend(this_alias_mentions)

    # Remove duplicates
    mentions = list(set(filtered_mentions))
    # Remove empty mentions
    mentions = [m for m in mentions if m[0]]

    return mentions

def load_model(model_name, num_threads=1):
    if torch.cuda.is_available():
        print("\t🧠 Using CUDA GPU for inference")
        device = torch.device("cuda")
    elif torch.backends.mps.is_available():
        print("\t🧠 Using Apple MPS GPU for inference")
        device = torch.device("mps")
    else:
        print("\t🧠 Using CPU for inference")
        device = torch.device("cpu")
        torch.set_num_threads(num_threads)

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name).to(device)
    model.eval()

    return (tokenizer, model, device)

def classify_mentions(pmcid, pmid, candidate_pairs, tokenizer=None, model=None, device=None):
    predictions = []

    for sentence, alias, resource in tqdm(candidate_pairs, desc="🔍 Classifying"):
        inputs = tokenizer(alias, sentence, return_tensors="pt", truncation=True, padding="max_length", max_length=512).to(device)
        with torch.no_grad():
            outputs = model(**inputs)
            probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
            pred = torch.argmax(probs, dim=1).item()
            if pred == 1:
                predictions.append({
                    "prediction": 1,
                    "pmcid": pmcid,
                    "pmid": pmid,
                    "resource_name": resource,
                    "matched_alias": alias,
                    "sentence": sentence,
                    "confidence": probs[0, 1].item()
                })
            else:
                predictions.append({
                    "prediction": 0,
                    "pmcid": pmcid,
                    "pmid": pmid,
                    "resource_name": resource,
                    "matched_alias": alias,
                    "sentence": sentence,
                    "confidence": probs[0, 0].item()
                })

    return predictions
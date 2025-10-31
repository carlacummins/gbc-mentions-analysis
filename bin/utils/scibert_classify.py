#!/usr/bin/env python3

import re
from collections import Counter
from tqdm import tqdm
import torch
from nltk.tokenize import sent_tokenize
from transformers import AutoTokenizer, AutoModelForSequenceClassification

VERBOSE = False

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
def get_resource_mentions_separate(textblocks, tableblocks, resource_names, case_sensitive_resources=[]):
    mentions = []

    # precompile regex patterns for each resource alias
    # This is more efficient than compiling them on-the-fly in the loop
    compiled_patterns = []
    for resource in resource_names:
        resource_name = resource[0]
        for alias in resource:
            if resource_name in case_sensitive_resources:
                pattern_case_sensitive = re.compile(rf"[^A-Za-z]{re.escape(alias)}[^A-Za-z]")
                compiled_patterns.append((resource_name, alias, pattern_case_sensitive))
            else:
                # Use case-insensitive pattern for all other resources
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
            if VERBOSE:
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

def _normalize_alias_for_regex(alias: str) -> str:
    """
    Turn a resource alias into a regex-safe pattern that matches flexibly.
      - Spaces -> \s+   (any whitespace)
      - Hyphens/dashes -> a class of common Unicode dashes
      - Dots -> \.?     (optional literal dot)
    Returns a regex string (ready for insertion into a larger pattern).
    """
    # Escape first so regex metachars in alias are treated literally
    escaped = re.escape(alias)

    # Replace escaped space ('\\ ') with regex \s+
    escaped = escaped.replace(r'\ ', r'\s+')

    # Replace escaped dash ('\-') with a set of dash-like characters
    dash_class = r'[-\u2010\u2011\u2012\u2013\u2014\u2212]'
    escaped = escaped.replace(r'\-', dash_class)

    # Replace escaped dot ('\.') with optional dot pattern
    escaped = escaped.replace(r'\.', r'\.?')

    return escaped

def get_resource_mentions(text, resource_names, case_sensitive_resources=[]):
    mentions = []

    # precompile regex patterns for each resource alias
    # This is more efficient than compiling them on-the-fly in the loop
    compiled_patterns = []
    for resource in resource_names:
        resource_name = resource[0]
        for alias in resource:
            if resource_name in case_sensitive_resources:
                alias_norm = _normalize_alias_for_regex(alias)
                pattern_case_sensitive = re.compile(rf"(?<![A-Za-z]){alias_norm}(?![A-Za-z])")
                compiled_patterns.append((resource_name, alias, pattern_case_sensitive, True))
            else:
                # Use case-insensitive pattern for all other resources
                alias_norm = _normalize_alias_for_regex(alias.lower())
                pattern_case_insensitive = re.compile(rf"(?<![A-Za-z]){alias_norm}(?![A-Za-z])")
                compiled_patterns.append((resource_name, alias, pattern_case_insensitive, False))

    # Tokenize the text into sentences and search for resource names
    sentences = sent_tokenize(text)  # Use NLTK to split into sentences
    for sentence in sentences:
        sentence = sentence.replace("\n", " ")
        s_lowered = sentence.lower()
        this_sentence_mentions = []
        for resource_name, alias, pattern, is_case_sensitive in compiled_patterns:
            if not is_case_sensitive and pattern.search(s_lowered):
                this_sentence_mentions.append((sentence.strip(), alias, resource_name))
            elif is_case_sensitive and pattern.search(sentence):
                this_sentence_mentions.append((sentence.strip(), alias, resource_name))

        if len(this_sentence_mentions) > 1:
            this_sentence_mentions = _remove_substring_matches(this_sentence_mentions)
        mentions.extend(this_sentence_mentions)

    # if a large number of matches are found for one resource, switch to case sensitive mode
    filtered_mentions = []
    alias_counts = Counter([m[1] for m in mentions])
    for alias, count in alias_counts.items():
        if count > case_sensitive_threshold and alias not in case_sensitive_resources:
            if VERBOSE:
                print(f"⚠️ {count} matches found for {alias} - switching to case sensitive mode")
            pattern_case_sensitive = re.compile(rf"(?<![A-Za-z]){re.escape(alias)}(?![A-Za-z])")
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
        if VERBOSE:
            print("\t🧠 Using CUDA GPU for inference")
        device = torch.device("cuda")
    elif torch.backends.mps.is_available():
        if VERBOSE:
            print("\t🧠 Using Apple MPS GPU for inference")
        device = torch.device("mps")
    else:
        if VERBOSE:
            print("\t🧠 Using CPU for inference")
        device = torch.device("cpu")
        torch.set_num_threads(num_threads)

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name).to(device)
    model.eval()

    return (tokenizer, model, device)

def classify_mentions(this_id, candidate_pairs, tokenizer=None, model=None, device=None):
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
                    "id": this_id,
                    "resource_name": resource,
                    "matched_alias": alias,
                    "sentence": sentence,
                    "confidence": probs[0, 1].item()
                })
            else:
                predictions.append({
                    "prediction": 0,
                    "id": this_id,
                    "resource_name": resource,
                    "matched_alias": alias,
                    "sentence": sentence,
                    "confidence": probs[0, 0].item()
                })

    return predictions

from transformers import AutoTokenizer

# Load tokenizer (SciBERT or any BERT-based model)
tokenizer = AutoTokenizer.from_pretrained("allenai/scibert_scivocab_uncased")

def chunk_text(text, matched_term, max_length=512, stride=128):
    """
    Splits a long input into overlapping chunks for transformer input.

    Args:
        text (str): The paragraph or article sentence.
        matched_term (str): The matched alias to include with [SEP].
        max_length (int): Max token length (default 512).
        stride (int): Overlap between chunks (default 128).

    Returns:
        List of tokenized input dictionaries for model input.
    """
    input_text = text + " [SEP] " + matched_term
    encodings = tokenizer(
        input_text,
        return_overflowing_tokens=True,
        max_length=max_length,
        truncation=True,
        stride=stride,
        padding="max_length"
    )

    return encodings["input_ids"], encodings["attention_mask"]

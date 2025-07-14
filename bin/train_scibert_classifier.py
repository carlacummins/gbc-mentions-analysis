
from transformers import AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from datasets import Dataset
import pandas as pd
import sys

# Load data
df = pd.read_csv(sys.argv[1])  # Expecting the CSV file path as the first argument
if df.empty:
    raise ValueError("The input CSV file is empty or not formatted correctly.")
dataset = Dataset.from_pandas(df[["input_text", "label"]])

# Tokenizer & model
model_name = "allenai/scibert_scivocab_uncased"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name, num_labels=2)

def tokenize(batch):
    return tokenizer(batch["input_text"], padding="max_length", truncation=True)

encoded_dataset = dataset.map(tokenize, batched=True)
encoded_dataset = encoded_dataset.train_test_split(test_size=0.2)

# Training setup
args = TrainingArguments(
    output_dir="./scibert_resource_classifier",
    evaluation_strategy="epoch",
    save_strategy="epoch",
    learning_rate=2e-5,
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8,
    num_train_epochs=4,
    weight_decay=0.01,
)

trainer = Trainer(
    model=model,
    args=args,
    train_dataset=encoded_dataset["train"],
    eval_dataset=encoded_dataset["test"],
)

trainer.train()

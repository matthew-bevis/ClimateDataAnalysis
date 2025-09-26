import os
import json

CHECKPOINT_FILE = "processed_files.jsonl"

def load_processed():
    """Return a set of already processed S3 keys."""
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE, "r") as f:
        return {json.loads(line)["key"] for line in f}

def mark_processed(key):
    """Append a processed file key to the checkpoint log."""
    record = {"key": key}
    with open(CHECKPOINT_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")

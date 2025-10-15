import os
import json

CHECKPOINT_FILE = "processed_files.jsonl"

def load_processed():
    """Return a set of already processed S3 keys, tolerating corrupt lines."""
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    keys = set()
    with open(CHECKPOINT_FILE, "r") as f:
        for line in f:
            try:
                obj = json.loads(line)
                if "key" in obj:
                    keys.add(obj["key"])
            except json.JSONDecodeError:
                continue
    return keys

def mark_processed(key):
    """Append a processed file key to the checkpoint log."""
    record = {"key": key}
    with open(CHECKPOINT_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")

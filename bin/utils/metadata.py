# utils/__init__.py

import os
import gzip
import json
import hashlib

default_shard_count = 128

def shard_key(article_id, shards=default_shard_count):
    return int(hashlib.md5(article_id.encode()).hexdigest(), 16) % shards

def shard_path(k, basepath='', shards=default_shard_count):
    width = max(2, len(str(max(1, shards) - 1)))
    return os.path.join(basepath, f"metadata_shard_{k:0{width}d}.jsonl.gz")

# Optional in-module cache so repeated lookups in the same shard are fast
_shard_cache = {}
def get_article_metadata(article_id, basepath='', shards=default_shard_count):
    global _shard_cache
    """
    Return the metadata dict for `article_id` from sharded JSONL.gz files under `basepath`.
    Expects lines like: {"pmc_id": "...", "meta": {...}}.
    """
    k = shard_key(article_id, shards)
    if k not in _shard_cache:
        shard_file = shard_path(k, basepath=basepath, shards=shards)
        shard_map = {}
        if os.path.exists(shard_file):
            with gzip.open(shard_file, 'rt', encoding='utf-8') as fh:
                for line in fh:
                    try:
                        rec = json.loads(line)
                        pid = rec.get('id')
                        if pid is not None:
                            shard_map[str(pid)] = rec.get('meta') or rec
                    except Exception:
                        # swallow bad lines but keep going; optionally log if you want
                        pass

        _shard_cache = {} # Reset the cache for this shard - only hold 1 shard at a time in memory
        _shard_cache[k] = shard_map
    return _shard_cache[k].get(str(article_id))

def sort_ids_by_shard(ids_iterable, shards=default_shard_count):
    """Return IDs sorted so that those sharing a shard are contiguous."""
    return sorted(ids_iterable, key=lambda _id: shard_key(str(_id), shards))
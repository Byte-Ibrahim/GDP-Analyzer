"""
plugins/inputs.py
-----------------
Phase 3: Generic, schema-driven CSV reader.

- Reads column mappings from config (schema_mapping)
- Casts each field to the correct type (string/integer/float)
- Completely domain-agnostic — works for ANY dataset
- Feeds packets into raw_queue one by one with configurable delay
- Does NOT know anything about Core internals
"""

import csv
import time
import multiprocessing
from typing import Any


# ── Type casters ──────────────────────────────────────────────────────
TYPE_CASTERS = {
    "string":  str,
    "integer": int,
    "float":   float,
}


def input_process(config: dict, raw_queue: multiprocessing.Queue) -> None:
    """
    Reads CSV row by row, maps columns to internal names,
    casts types, and pushes packets into raw_queue.
    Runs as a standalone process (no class needed — plain function).
    """
    dataset_path   = config.get("dataset_path", "data/data.csv")
    delay          = config.get("pipeline_dynamics", {}).get("input_delay_seconds", 0.01)
    schema_columns = config.get("schema_mapping", {}).get("columns", [])

    # Build mapping: source_name -> (internal_mapping, caster)
    column_map = {
        col["source_name"]: (col["internal_mapping"], TYPE_CASTERS.get(col["data_type"], str))
        for col in schema_columns
    }

    try:
        with open(dataset_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                packet = {}
                valid = True

                for source_name, (internal_name, caster) in column_map.items():
                    raw_val = row.get(source_name, "").strip()
                    try:
                        packet[internal_name] = caster(raw_val)
                    except (ValueError, TypeError):
                        valid = False
                        break

                if valid:
                    raw_queue.put(packet)
                    time.sleep(delay)

    except FileNotFoundError:
        print(f"[InputProcess] File not found: {dataset_path}")

    finally:
        # Send poison pill to shut down workers
        raw_queue.put(None)
        print("[InputProcess] Done — poison pill sent.")
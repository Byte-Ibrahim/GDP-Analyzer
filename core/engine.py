"""
core/engine.py
--------------
Phase 3 Core Engine.

Architecture:
  - Worker processes pull from raw_queue (Input -> Core stream)
  - Each worker runs stateless signature verification (Scatter)
  - Verified packets pushed to processed_queue (Core -> Output stream)
  - Aggregator process pulls from processed_queue, runs stateful sliding window (Gather)
  - Results pushed to result_queue for the dashboard

Patterns used:
  - Scatter-Gather (parallel workers + single aggregator)
  - Functional Core, Imperative Shell (pure functions for logic, process loop as shell)
"""

import hashlib
import multiprocessing
import time
from collections import deque
from typing import Any, List, Optional


# ═══════════════════════════════════════════════════════════════════════ #
#  FUNCTIONAL CORE — Pure functions, no side effects, fully testable     #
# ═══════════════════════════════════════════════════════════════════════ #

def compute_signature(value: float, secret_key: str, iterations: int) -> str:
    raw_value_str = f"{value:.2f}"
    password_bytes = secret_key.encode("utf-8")
    salt_bytes = raw_value_str.encode("utf-8")
    
    hash_bytes = hashlib.pbkdf2_hmac(
        hash_name="sha256",
        password=password_bytes,
        salt=salt_bytes,
        iterations=iterations
    )
    return hash_bytes.hex()


def verify_packet(packet: dict, secret_key: str, iterations: int) -> bool:
    """
    Pure function: Returns True if packet's security_hash matches computed signature.
    Stateless — no side effects.
    """
    try:
        value = packet.get("metric_value")
        expected = compute_signature(float(value), secret_key, iterations)
        actual = packet.get("security_hash", "")
        return expected == actual
    except (TypeError, ValueError):
        return False


def compute_running_average(window: deque) -> float:
    """
    Pure function: Computes average of values in the sliding window.
    Functional Core — takes data, returns result, touches no state.
    """
    if not window:
        return 0.0
    return round(sum(window) / len(window), 4)


def enrich_packet(packet: dict, avg: float) -> dict:
    """
    Pure function: Returns a new packet dict with computed_metric added.
    Does not mutate the original.
    """
    return {**packet, "computed_metric": avg}


# ═══════════════════════════════════════════════════════════════════════ #
#  IMPERATIVE SHELL — Worker process loops (handle queues/state/I/O)    #
# ═══════════════════════════════════════════════════════════════════════ #

def worker_process(raw_queue: multiprocessing.Queue,
                   processed_queue: multiprocessing.Queue,
                   secret_key: str,
                   iterations: int,
                   worker_id: int):
    """
    Imperative Shell: Worker process.
    Pulls packets from raw_queue, calls pure verify_packet,
    pushes verified packets to processed_queue.
    Drops unverified packets silently.
    """
    while True:
        try:
            packet = raw_queue.get(timeout=2)
            if packet is None:          # Poison pill — shut down
                raw_queue.put(None)     # Pass pill to next worker
                break

            # ── Functional Core called here ──
            if verify_packet(packet, secret_key, iterations):
                processed_queue.put(packet)
            # else: packet is dropped (unverified)

        except Exception:
            break


def aggregator_process(processed_queue: multiprocessing.Queue,
                       result_queue: multiprocessing.Queue,
                       window_size: int):
    """
    Imperative Shell: Aggregator process.
    Pulls verified packets, maintains sliding window (mutable state here — imperative),
    calls pure compute_running_average for the actual calculation,
    pushes enriched results to result_queue for the dashboard.
    """
    window = deque(maxlen=window_size)   # Mutable state lives here in the shell

    while True:
        try:
            packet = processed_queue.get(timeout=2)
            if packet is None:
                result_queue.put(None)   # Signal dashboard to stop
                break

            # ── Imperative Shell: update state ──
            window.append(packet["metric_value"])

            # ── Functional Core: compute average (pure) ──
            avg = compute_running_average(window)

            # ── Functional Core: enrich packet (pure) ──
            enriched = enrich_packet(packet, avg)

            result_queue.put(enriched)

        except Exception:
            break
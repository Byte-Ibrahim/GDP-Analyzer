"""
main.py
-------
Phase 3 Orchestrator — Producer-Consumer Pipeline with Multiprocessing.

Architecture:
                    raw_queue          processed_queue      result_queue
  [InputProcess] ─────────► [Worker x N] ──────────► [Aggregator] ──────► [Dashboard]
                                  ▲ Scatter-Gather ▲
                                        │
                              [PipelineTelemetry] ──► telemetry_queue ──► [Dashboard]

Dependency Injection order:
  1. Create Queues
  2. Start Telemetry monitor (Subject)
  3. Start Dashboard process (Observer subscribed to telemetry)
  4. Start Aggregator process
  5. Start N Worker processes  (Scatter)
  6. Start Input process       (Producer)
"""

import json
import sys
import multiprocessing
import threading
import time

from core.engine    import worker_process, aggregator_process
from core.telemetry import PipelineTelemetry
from plugins.inputs  import input_process
from plugins.outputs import dashboard_process, TelemetryObserver


def load_config(path: str = "config.json") -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] config.json not found at: {path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON: {e}")
        sys.exit(1)


def bootstrap():
    config = load_config("config.json")

    dynamics    = config.get("pipeline_dynamics", {})
    parallelism = int(dynamics.get("core_parallelism", 4))
    max_size    = int(dynamics.get("stream_queue_max_size", 50))

    processing  = config.get("processing", {})
    stateless   = processing.get("stateless_tasks", {})
    stateful    = processing.get("stateful_tasks", {})

    secret_key  = stateless.get("secret_key", "")
    iterations  = int(stateless.get("iterations", 100000))
    window_size = int(stateful.get("running_average_window_size", 10))

    # ── 1. Create the three queues ────────────────────────────────────
    raw_queue       = multiprocessing.Queue(maxsize=max_size)
    processed_queue = multiprocessing.Queue(maxsize=max_size)
    result_queue    = multiprocessing.Queue(maxsize=max_size)
    telemetry_queue = multiprocessing.Queue(maxsize=100)

    print("[Bootstrap] Queues created.")

    # ── 2. Telemetry monitor (Subject) ────────────────────────────────
    telemetry = PipelineTelemetry(raw_queue, processed_queue, result_queue, max_size)

    # Telemetry pushes snapshots into telemetry_queue for the dashboard process
    class QueueTelemetryObserver:
        def on_telemetry_update(self, snap: dict) -> None:
            try:
                telemetry_queue.put_nowait(snap)
            except Exception:
                pass  # Queue full — skip this snapshot

    telemetry.subscribe(QueueTelemetryObserver())
    telemetry.start()
    print("[Bootstrap] Telemetry monitor started.")

    # ── 3. Dashboard process ──────────────────────────────────────────
    dash_proc = multiprocessing.Process(
        target=dashboard_process,
        args=(result_queue, telemetry_queue, config),
        daemon=False
    )
    dash_proc.start()
    print("[Bootstrap] Dashboard process started.")

    # ── 4. Aggregator process (Gather node) ───────────────────────────
    agg_proc = multiprocessing.Process(
        target=aggregator_process,
        args=(processed_queue, result_queue, window_size),
        daemon=True
    )
    agg_proc.start()
    print("[Bootstrap] Aggregator process started.")

    # ── 5. Worker processes (Scatter — N parallel verifiers) ──────────
    workers = []
    for i in range(parallelism):
        w = multiprocessing.Process(
            target=worker_process,
            args=(raw_queue, processed_queue, secret_key, iterations, i),
            daemon=True
        )
        w.start()
        workers.append(w)
    print(f"[Bootstrap] {parallelism} worker processes started.")

    # ── 6. Input process (Producer) ───────────────────────────────────
    inp_proc = multiprocessing.Process(
        target=input_process,
        args=(config, raw_queue),
        daemon=True
    )
    inp_proc.start()
    print("[Bootstrap] Input process started.")
    print("[Bootstrap] Pipeline running — close the dashboard window to exit.\n")

    # ── Wait for dashboard to finish ──────────────────────────────────
    dash_proc.join()
    telemetry.stop()
    print("[Bootstrap] Pipeline complete.")


if __name__ == "__main__":
    multiprocessing.freeze_support()   # Required for Windows
    bootstrap()
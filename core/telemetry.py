"""
core/telemetry.py
-----------------
PipelineTelemetry — the Subject in the Observer Pattern.

Monitors queue sizes independently.
Dashboard (Observer) subscribes to receive updates.
Does NOT import any plugin — DIP compliant.
"""

import multiprocessing
import threading
import time
from typing import List, Any


class PipelineTelemetry:
    """
    Subject: Polls queue sizes and notifies all subscribed observers.
    Runs in its own daemon thread so it doesn't block the pipeline.
    """

    def __init__(self, raw_queue, processed_queue, result_queue, max_size: int):
        self.raw_queue       = raw_queue
        self.processed_queue = processed_queue
        self.result_queue    = result_queue
        self.max_size        = max_size
        self._observers      = []
        self._running        = False

    def subscribe(self, observer) -> None:
        """Observer registers itself here."""
        self._observers.append(observer)

    def _notify(self, telemetry: dict) -> None:
        """Push telemetry snapshot to all observers."""
        for observer in self._observers:
            observer.on_telemetry_update(telemetry)

    def start(self) -> None:
        """Start polling in a background thread."""
        self._running = True
        thread = threading.Thread(target=self._poll_loop, daemon=True)
        thread.start()

    def stop(self) -> None:
        self._running = False

    def _poll_loop(self) -> None:
        while self._running:
            try:
                raw_size       = self.raw_queue.qsize()
                processed_size = self.processed_queue.qsize()
                result_size    = self.result_queue.qsize()

                telemetry = {
                    "raw_queue_size":       raw_size,
                    "processed_queue_size": processed_size,
                    "result_queue_size":    result_size,
                    "max_size":             self.max_size,
                    "raw_pct":              min(raw_size / max(self.max_size, 1), 1.0),
                    "processed_pct":        min(processed_size / max(self.max_size, 1), 1.0),
                    "result_pct":           min(result_size / max(self.max_size, 1), 1.0),
                }
                self._notify(telemetry)
            except Exception:
                pass
            time.sleep(0.5)
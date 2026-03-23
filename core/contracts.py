"""
core/contracts.py
-----------------
All Protocols (contracts) are owned by the Core.
Phase 3: Extended with queue-based streaming contracts.
"""

from typing import Protocol, List, Any, runtime_checkable
import multiprocessing


@runtime_checkable
class DataSink(Protocol):
    """Outbound Abstraction — anything that can receive a write() call."""
    def write(self, report_type: str, data: Any) -> None:
        ...


class PipelineService(Protocol):
    """Inbound Abstraction — anything that can receive raw data packets."""
    def execute(self, raw_data: List[Any]) -> None:
        ...


class TelemetryObserver(Protocol):
    """Observer contract — dashboard subscribes to telemetry updates."""
    def on_telemetry_update(self, telemetry: dict) -> None:
        ...
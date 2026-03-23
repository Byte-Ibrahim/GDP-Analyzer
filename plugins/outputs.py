"""
plugins/outputs.py
------------------
Phase 3: Real-time Matplotlib Dashboard.

- Implements TelemetryObserver (Observer Pattern)
- Shows live line charts for metric_value and computed_metric
- Shows color-coded telemetry bars (Green/Yellow/Red) for all 3 queues
- Runs as a standalone process pulling from result_queue
- Domain-agnostic: uses x_axis / y_axis keys from config
"""

import multiprocessing
import time
from collections import deque
from typing import Any


# ── Color thresholds for backpressure visualization ───────────────────
def _queue_color(pct: float) -> str:
    if pct < 0.5:
        return "#2ecc71"   # Green  — flowing smoothly
    elif pct < 0.8:
        return "#f39c12"   # Yellow — filling up
    else:
        return "#e74c3c"   # Red    — heavy backpressure


# ═══════════════════════════════════════════════════════════════════════ #
#  TelemetryObserver                                                      #
# ═══════════════════════════════════════════════════════════════════════ #
class TelemetryObserver:
    """
    Observer: Receives telemetry snapshots from PipelineTelemetry (Subject).
    Stores latest snapshot for the dashboard to read.
    """
    def __init__(self):
        self.latest = {}

    def on_telemetry_update(self, telemetry: dict) -> None:
        self.latest = telemetry


# ═══════════════════════════════════════════════════════════════════════ #
#  Dashboard Process                                                      #
# ═══════════════════════════════════════════════════════════════════════ #
def dashboard_process(result_queue: multiprocessing.Queue,
                      telemetry_queue: multiprocessing.Queue,
                      config: dict) -> None:
    """
    Runs as a standalone process.
    Pulls enriched packets from result_queue and renders live charts.
    Pulls telemetry snapshots from telemetry_queue and updates queue bars.
    """
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec
    from matplotlib.patches import FancyBboxPatch

    vis_config  = config.get("visualizations", {})
    charts_cfg  = vis_config.get("data_charts", [])
    telemetry_cfg = vis_config.get("telemetry", {})

    # Determine axis keys from config
    chart_values = next((c for c in charts_cfg if c["type"] == "real_time_line_graph_values"), {})
    chart_avg    = next((c for c in charts_cfg if c["type"] == "real_time_line_graph_average"), {})

    x_key      = chart_values.get("x_axis", "time_period")
    y_key      = chart_values.get("y_axis", "metric_value")
    avg_y_key  = chart_avg.get("y_axis", "computed_metric")

    # Data buffers
    x_data   = deque(maxlen=100)
    y_data   = deque(maxlen=100)
    avg_data = deque(maxlen=100)

    # Telemetry state
    telemetry = {"raw_pct": 0.0, "processed_pct": 0.0, "result_pct": 0.0,
                 "raw_queue_size": 0, "processed_queue_size": 0, "result_queue_size": 0,
                 "max_size": 50}

    # ── Build figure layout ───────────────────────────────────────────
    plt.ion()
    fig = plt.figure(figsize=(14, 9))
    fig.suptitle("🌐 Real-Time Pipeline Dashboard", fontsize=14, fontweight="bold")

    gs = gridspec.GridSpec(3, 2, figure=fig,
                           height_ratios=[1, 3, 3],
                           hspace=0.45, wspace=0.35)

    # Row 0: Telemetry bars (spans both columns)
    ax_tel = fig.add_subplot(gs[0, :])
    ax_tel.set_title("Pipeline Stream Telemetry", fontsize=10)
    ax_tel.set_xlim(0, 1)
    ax_tel.set_ylim(-0.5, 2.5)
    ax_tel.axis("off")

    # Row 1: Live values chart
    ax_val = fig.add_subplot(gs[1, :])
    ax_val.set_title(chart_values.get("title", "Live Values"), fontsize=10)
    ax_val.set_xlabel(x_key)
    ax_val.set_ylabel(y_key)
    line_val, = ax_val.plot([], [], "b-o", markersize=3, linewidth=1.5)

    # Row 2: Running average chart
    ax_avg = fig.add_subplot(gs[2, :])
    ax_avg.set_title(chart_avg.get("title", "Running Average"), fontsize=10)
    ax_avg.set_xlabel(x_key)
    ax_avg.set_ylabel(avg_y_key)
    line_avg, = ax_avg.plot([], [], "g-o", markersize=3, linewidth=1.5)

    plt.tight_layout(rect=[0, 0, 1, 0.95])

    packets_received = 0
    packets_dropped  = 0

    def draw_telemetry():
        ax_tel.cla()
        ax_tel.axis("off")
        ax_tel.set_title("Pipeline Stream Telemetry (Green=OK  Yellow=Filling  Red=Backpressure)",
                          fontsize=9)

        show_raw  = telemetry_cfg.get("show_raw_stream", True)
        show_mid  = telemetry_cfg.get("show_intermediate_stream", True)
        show_proc = telemetry_cfg.get("show_processed_stream", True)

        bars = []
        if show_raw:
            bars.append(("Raw Queue",       telemetry["raw_pct"],       telemetry["raw_queue_size"]))
        if show_mid:
            bars.append(("Processed Queue", telemetry["processed_pct"], telemetry["processed_queue_size"]))
        if show_proc:
            bars.append(("Result Queue",    telemetry["result_pct"],    telemetry["result_queue_size"]))

        bar_h   = 0.3
        spacing = 1.0 / max(len(bars), 1)

        for i, (label, pct, size) in enumerate(bars):
            y_pos  = 1 - (i + 0.5) * spacing
            color  = _queue_color(pct)
            # Background bar
            ax_tel.barh(y_pos, 1.0, height=bar_h, color="#ecf0f1", left=0)
            # Fill bar
            ax_tel.barh(y_pos, max(pct, 0.01), height=bar_h, color=color, left=0)
            ax_tel.text(-0.01, y_pos, label, ha="right", va="center", fontsize=8)
            ax_tel.text(1.01,  y_pos, f"{size}/{telemetry['max_size']}",
                        ha="left", va="center", fontsize=8)

        ax_tel.set_xlim(-0.25, 1.2)
        ax_tel.set_ylim(0, 1)

    def update_charts():
        if x_data:
            xs = list(x_data)
            line_val.set_data(xs, list(y_data))
            ax_val.relim()
            ax_val.autoscale_view()

            line_avg.set_data(xs, list(avg_data))
            ax_avg.relim()
            ax_avg.autoscale_view()

        fig.canvas.draw_idle()
        fig.canvas.flush_events()

    # ── Main dashboard loop ───────────────────────────────────────────
    while True:
        # Drain result_queue
        try:
            while True:
                packet = result_queue.get_nowait()
                if packet is None:
                    print(f"\n[Dashboard] Pipeline complete. "
                          f"Received={packets_received}, Dropped={packets_dropped}")
                    draw_telemetry()
                    update_charts()
                    plt.ioff()
                    plt.show()
                    return

                x_data.append(packet.get(x_key, 0))
                y_data.append(packet.get(y_key, 0))
                avg_data.append(packet.get(avg_y_key, 0))
                packets_received += 1

        except Exception:
            pass

        # Drain telemetry_queue
        try:
            while True:
                snap = telemetry_queue.get_nowait()
                telemetry.update(snap)
        except Exception:
            pass

        draw_telemetry()
        update_charts()
        time.sleep(0.1)
Project Phase 3: Generic Concurrent Real-Time Data Pipeline
-----------------------------------------------------------

Overview:
---------
This project implements a fully generic, multiprocessing, real-time data processing pipeline.
It reads CSV datasets, processes data in parallel workers, computes running averages, and visualizes 
both raw and processed data in a live dashboard. The system is entirely **configuration-driven** 
using `config.json` and follows the **Dependency Inversion Principle**.

Pipeline Architecture:
----------------------
                      raw_queue          processed_queue      	    result_queue
    [InputProcess] 	---> 	[Worker x N] 	--->	 [Aggregator] 	---> 	[Dashboard]
                                      Scatter-Gather 
                                           │
                                 [PipelineTelemetry] ──► telemetry_queue ──► [Dashboard]

- **InputProcess**: Reads CSV, maps columns using schema, casts types, pushes packets to raw_queue.
- **Worker Processes (Scatter)**: Verify packets (e.g., signature) in parallel, push verified packets to processed_queue.
- **Aggregator (Gather)**: Maintains sliding window, computes running average, enriches packets, pushes to result_queue.
- **Dashboard**: Reads enriched results and telemetry snapshots, displays live charts and queue backpressure.
- **PipelineTelemetry**: Monitors all queues independently and updates observers (dashboard) via Observer Pattern.

Directory Structure:
--------------------
root/
│
├── main.py               # Entry point: orchestrates the pipeline, starts all processes
├── config.json           # Pipeline configuration (dataset path, schema mapping, processing, visualization)
├── data/                 # Place your CSV datasets here
│    └── sample_sensor_data.csv  # Example dataset
├── core/                 # Core modules (pure logic, engine, telemetry, contracts)
│    ├── engine.py
│    ├── telemetry.py
│    └── contracts.py
├── plugins/              # Input and Output modules (domain-agnostic)
│    ├── inputs.py        # Reads CSV and pushes packets to raw_queue
│    └── outputs.py       # Dashboard + Observer implementation
└── readme.txt            # This file

Requirements:
-------------
- Python 3.10+ (Windows/Linux/Mac)
- Libraries: `matplotlib` (for the dashboard)
- Standard Python libraries: `multiprocessing`, `threading`, `csv`, `hashlib`, `json`, `collections`, `time`

Configuration (`config.json`):
-------------------------------
The system is entirely driven by `config.json`. You must define:

1. **Dataset**
   - `"dataset_path"`: relative path to your CSV file.
2. **Pipeline Dynamics**
   - `"input_delay_seconds"`: delay between reading rows (simulates streaming)
   - `"core_parallelism"`: number of worker processes
   - `"stream_queue_max_size"`: max queue size (controls backpressure)
3. **Schema Mapping**
   - Maps CSV column names to internal variables (`entity_name`, `time_period`, `metric_value`, `security_hash`)
   - Sets type casting (`string`, `integer`, `float`)
4. **Processing**
   - `stateless_tasks`: e.g., signature verification (pure function)
   - `stateful_tasks`: e.g., running average over sliding window
5. **Visualizations**
   - Real-time charts for values and running averages
   - Queue telemetry bars: Green = OK, Yellow = filling, Red = backpressure

Example Config:
---------------
Refer to `config.json` included in this project.

Running the Pipeline:
---------------------
1. Place your CSV dataset(s) in the `data/` folder.
2. Update `config.json` with the correct dataset path and schema.
3. Open terminal/command prompt in the root folder.
4. Run:

   Windows:
       python main.py
   Linux/Mac:
       python3 main.py

5. Close the dashboard window to stop the pipeline gracefully.

Testing:
--------
- Use the provided `sample_sensor_data.csv`.
- Signature verification can be disabled in `config.json` using:
      "stateless_tasks": { "operation": "none" }
- Running average window can be adjusted in:
      "stateful_tasks": { "running_average_window_size": N }

Notes:
------
- The pipeline is **domain-agnostic**: works with any dataset that matches the schema mapping.
- Designed using **Functional Core / Imperative Shell** and **Scatter-Gather** patterns.
- Telemetry monitors queue health independently and updates the dashboard via the Observer Pattern.
- Poison pills (`None`) are used to gracefully shut down workers and aggregator.

Contact:
--------
For questions or issues, contact: [Your Name / Email]
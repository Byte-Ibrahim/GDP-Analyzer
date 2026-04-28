Software Design & Architecture (SDA) Project
This project implements a high-performance, domain-agnostic data engine designed to process and analyze global economic indicators. The development followed a three-phase architectural roadmap, transitioning from a basic functional script to a sophisticated, concurrent system built on SOLID principles.

🏗️ Architectural Roadmap
Phase 1: Configuration-Driven Functional Design
Focus: Logic foundation and functional programming.

Architecture: Developed a core engine using Python to perform initial GDP data analysis.

Key Feature: Implemented a system where behavior is dictated by external configuration files, allowing for basic data flexibility.

Phase 2: Modularity & Dependency Inversion (DIP)
Focus: Decoupling and structural resilience.

Architecture: Refactored the codebase to adhere to the Dependency Inversion Principle.

Key Components:

Data Loaders: Abstracted source handling to support multiple formats.

Dashboard Observers: Implemented the Observer Pattern to decouple the data processing engine from the visualization/reporting UI.

Interfaces: Shifted toward a modular design where high-level modules do not depend on low-level implementations.

Phase 3: High-Throughput Concurrent Pipeline
Focus: Scalability, concurrency, and real-time telemetry.

Architecture: Implemented a Producer-Consumer pipeline utilizing Python’s multiprocessing library.

Advanced Mechanics:

Sliding Window: Integrated a sliding window mechanism for time-series GDP analysis.

Real-time Telemetry: Added instrumentation to monitor pipeline health and data throughput.

Concurrency: Optimized for high-throughput processing to handle large datasets with minimal latency.

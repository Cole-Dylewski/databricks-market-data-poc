# Databricks Market Data POC

## Overview

This project is a proof-of-concept data engineering pipeline that demonstrates how financial market data can be ingested from multiple public and free data sources into Databricks using Apache Spark and Delta Lake. The primary goal is to showcase modern lakehouse design patterns, incremental ingestion techniques, and Spark-based analytical transformations in a Databricks environment.

The project supports multiple market data sources, including Alpaca (optional, requires API keys) and other public/free data sources that can be used without authentication. This makes the repository easily cloneable and usable by anyone without requiring API keys or secrets.

The project is intentionally scoped as a technical demonstration rather than a production system. It emphasizes clarity, correctness, and architectural best practices over enterprise-scale operational complexity.

---

## Objectives

* Demonstrate ingestion of external REST API data into Databricks
* Apply a medallion (bronze / silver / gold) data architecture
* Use Delta Lake for reliable, repeatable data processing
* Showcase Spark transformations, window functions, and incremental loads
* Present clean, interview-ready code and documentation suitable for a portfolio project

---

## Architecture

The pipeline follows a standard Databricks lakehouse pattern:

### Bronze Layer (Raw Ingestion)

* Data is pulled from multiple market data sources (e.g., Alpaca, Yahoo Finance, Alpha Vantage, etc.)
* Raw records are ingested with minimal transformation
* Metadata such as ingestion timestamp and data source are added
* Data is written append-only to Delta tables

### Silver Layer (Cleaned & Normalized)

* Schema enforcement and type casting
* Deduplication based on symbol and timestamp
* Standardized column names and data types
* Idempotent processing using Delta MERGE operations

### Gold Layer (Analytics & Aggregates)

* Daily OHLCV aggregations per symbol
* Simple technical indicators (e.g., moving averages, returns)
* Analytics-ready tables optimized for querying and visualization

---

## Technology Stack

* Python
* Apache Spark
* Delta Lake
* Databricks (Free Edition)
* Multiple Market Data Sources:
  * Alpaca Market Data API (optional, requires API keys)
  * Yahoo Finance (free, no API keys required)
  * Alpha Vantage (free tier available)
  * Other public/free market data APIs
* REST-based ingestion using `requests`

---

## Project Structure

```
databricks-market-data-poc/
├── README.md
├── notebooks/
│   ├── 00_setup.py
│   ├── 01_ingest_bronze_bars.py
│   ├── 02_transform_silver_bars.py
│   ├── 03_gold_analytics.py
│   └── 04_data_quality_checks.py
├── src/
│   ├── data_sources/          # Data source clients
│   │   ├── __init__.py
│   │   ├── alpaca_client.py   # Alpaca API client (optional)
│   │   ├── yahoo_finance.py   # Yahoo Finance client (free)
│   │   └── base_client.py     # Base class for data sources
│   ├── config.py
│   ├── schemas.py
│   ├── transforms.py
│   └── utils.py
├── requirements.txt
└── LICENSE
```

### Notebooks

* `00_setup.py`
  Environment setup, configuration loading, and shared utilities

* `01_ingest_bronze_bars.py`
  Pulls market bar data from configured data sources and writes raw records to bronze Delta tables

* `02_transform_silver_bars.py`
  Cleans, deduplicates, and normalizes bronze data into silver tables

* `03_gold_analytics.py`
  Builds aggregated and analytical datasets for downstream use

* `04_data_quality_checks.py`
  Basic data validation and sanity checks

### Source Code

* `data_sources/`
  Modular data source clients that can be easily extended:
  * `base_client.py`: Abstract base class defining the interface for all data sources
  * `alpaca_client.py`: Alpaca API client (optional, requires API keys)
  * `yahoo_finance.py`: Yahoo Finance client (free, no API keys required)
  * Additional data sources can be added by implementing the base client interface

* `schemas.py`
  Explicit Spark schemas used across ingestion and transformation layers

* `transforms.py`
  Reusable Spark transformation logic

* `utils.py`
  Utility functions for logging, date/time handling, and common operations

---

## Data Ingestion Approach

* Supports multiple market data sources with a unified interface
* Default configuration uses free data sources (e.g., Yahoo Finance) that require no API keys
* Optional support for Alpaca API (requires API keys) for enhanced data quality
* Supports parameterized symbols, timeframes, and time windows
* Designed for incremental ingestion (e.g., last N minutes/hours)
* Handles API rate limits with basic retry and error handling logic
* Data source selection is configurable, allowing users to choose which sources to use

Streaming and WebSocket ingestion are intentionally out of scope for this POC.

---

## Data Quality & Reliability

Although this is a demonstration project, basic quality checks are included:

* Non-null constraints on key fields (symbol, timestamp, close)
* Deduplication guarantees at the silver layer
* Simple row count and timestamp range validation
* Idempotent re-runs using Delta Lake semantics

---

## Security & Configuration

* API keys are not hardcoded
* Credentials are expected to be provided via environment variables or notebook-scoped configuration
* No secrets are committed to the repository
* **The project can be used without any API keys** by defaulting to free data sources (e.g., Yahoo Finance)
* Alpaca and other premium data sources are optional and only required if explicitly configured

---

## How to Run

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Cole-Dylewski/databricks-market-data-poc.git
   cd databricks-market-data-poc
   ```

2. **Import the project into a Databricks workspace:**
   - Use Databricks Repos to connect the repository (recommended), or
   - Manually upload the notebooks and source files

3. **Install dependencies:**
   - Install packages from `requirements.txt` in your Databricks cluster or environment
   - Note: PySpark and Delta Lake are typically pre-installed in Databricks

4. **Configure data sources (optional):**
   - By default, the project uses free data sources (e.g., Yahoo Finance) that require no API keys
   - To use Alpaca or other premium sources, set API credentials as environment variables or notebook parameters
   - See `src/config.py` for data source configuration options

5. **Run notebooks in order:**
   1. `00_setup.py`
   2. `01_ingest_bronze_bars.py`
   3. `02_transform_silver_bars.py`
   4. `03_gold_analytics.py`
   5. `04_data_quality_checks.py`

---

## Intended Audience

This project is designed for:

* Hiring managers evaluating data engineering candidates
* Interview discussions around Databricks, Spark, and lakehouse design
* Demonstrating architectural thinking and clean implementation practices

It is not intended for live trading, real-time analytics, or production deployment.

---

## Future Enhancements (Not Implemented)

* Databricks Jobs orchestration
* Structured Streaming ingestion
* Secret scopes / key vault integration
* CI/CD for notebooks and schemas
* Advanced data quality frameworks
* Visualization dashboards

---

## License

This project is licensed under the MIT License.

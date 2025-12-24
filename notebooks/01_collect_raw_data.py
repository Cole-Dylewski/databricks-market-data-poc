# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Data Collection: Scheduled Raw Data Ingestion
# MAGIC
# MAGIC **Production Pipeline - Step 1: Data Collection**
# MAGIC
# MAGIC This notebook is designed to run as a scheduled job (typically daily after market close).
# MAGIC It:
# MAGIC 1. Retrieves S&P 500 stock symbols from stockanalysis.com
# MAGIC 2. Fetches the last complete trading day's 5-minute bar data for each symbol
# MAGIC    (automatically skips weekends and NYSE holidays)
# MAGIC 3. Writes raw data to Databricks Volume (landing zone) as JSON files
# MAGIC
# MAGIC **Idempotent**: Can be safely re-run if needed (overwrites files for the same date)
# MAGIC
# MAGIC **Holiday Handling**: Uses `get_last_trading_day()` which accounts for all NYSE holidays

# COMMAND ----------

import sys
import os
import json
from pathlib import Path
from typing import Optional
from datetime import datetime, date

# Add project root to Python path for imports
def add_project_root_to_path() -> str:
    """Add project root directory to sys.path for imports.
    
    Works in Databricks Repos, Workspace files, and local environments.
    """
    project_root: Optional[Path] = None
    
    # Method 1: Try using __file__ (works when running as a Python script)
    try:
        if '__file__' in globals():
            notebook_dir = Path(__file__).parent
            if (notebook_dir / '01_collect_raw_data.py').exists():
                project_root = notebook_dir.parent
    except (NameError, AttributeError):
        pass
    
    # Method 2: Try using dbutils (Databricks-specific)
    if project_root is None:
        try:
            import dbutils
            # Try multiple ways to get the notebook path
            try:
                workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            except:
                try:
                    workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
                except:
                    workspace_path = None
            
            if workspace_path:
                # Handle both /Workspace/Repos/... and /Workspace/Users/... paths
                if workspace_path.startswith('/'):
                    # Remove leading slash and split
                    parts = workspace_path.lstrip('/').split('/')
                    if len(parts) >= 2:
                        # Try Repos path: /Workspace/Repos/user/repo/notebooks/...
                        if parts[0] == 'Workspace' and parts[1] == 'Repos':
                            if len(parts) >= 4:
                                repo_path = Path('/Workspace/Repos').joinpath(*parts[2:-1])
                                if (repo_path / "src").exists():
                                    project_root = repo_path
                        # Try Users path: /Workspace/Users/user/project/notebooks/...
                        elif parts[0] == 'Workspace' and parts[1] == 'Users':
                            if len(parts) >= 4:
                                user_path = Path('/Workspace/Users').joinpath(*parts[2:-1])
                                if (user_path / "src").exists():
                                    project_root = user_path
        except Exception as e:
            print(f"Note: Could not use dbutils path detection: {e}")
            pass
    
    # Method 3: Try current working directory and walk up
    if project_root is None:
        current = Path.cwd()
        max_depth = 15
        depth = 0
        while depth < max_depth:
            if (current / "src").exists() and (current / "notebooks").exists():
                project_root = current
                break
            if current.parent == current:
                break
            current = current.parent
            depth += 1
    
    # Method 4: Try common Databricks paths
    if project_root is None:
        common_paths = [
            Path("/Workspace/Repos"),
            Path("/Workspace/Users"),
            Path.cwd(),
        ]
        for base_path in common_paths:
            if base_path.exists():
                for item in base_path.iterdir():
                    if item.is_dir() and (item / "src").exists() and (item / "notebooks").exists():
                        project_root = item
                        break
                if project_root:
                    break
    
    if project_root is None:
        raise RuntimeError(
            "Could not determine project root directory. "
            "Please ensure you're running from within the project directory, "
            "or set the project root manually."
        )
    
    project_root_str = str(project_root.resolve())
    if project_root_str not in sys.path:
        sys.path.insert(0, project_root_str)
    
    print(f"Project root detected: {project_root_str}")
    print(f"Verifying src directory exists: {(Path(project_root_str) / 'src').exists()}")
    print(f"Verifying notebooks directory exists: {(Path(project_root_str) / 'notebooks').exists()}")
    
    return project_root_str

project_root = add_project_root_to_path()
print(f"Added project root to path: {project_root}")

# COMMAND ----------

# Install yfinance if not available (for Databricks)
# Note: Do NOT install delta-spark - Databricks has it built-in and pip install causes conflicts
try:
    import yfinance as yf
except ImportError:
    print("yfinance not found, installing...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance>=0.2.0"])
    import yfinance as yf
    print("yfinance installed successfully")

# Ensure delta-spark is NOT installed (Databricks has it built-in)
try:
    import subprocess
    import sys
    result = subprocess.run(
        [sys.executable, "-m", "pip", "show", "delta-spark"],
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print("[WARN] delta-spark found installed via pip. Uninstalling to avoid conflicts...")
        subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", "delta-spark"])
        print("[OK] Removed delta-spark (using Databricks built-in Delta Lake)")
except Exception:
    pass  # delta-spark not installed, which is fine

# COMMAND ----------

from src.utils import get_sp500_symbols, fetch_previous_day_5min_bars, get_last_trading_day
from src.config import DATABRICKS_PATHS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Initialize Spark Session

# COMMAND ----------

# Initialize Spark session for dbutils.fs operations (needed for writing to volumes)
# Note: We don't need Delta Lake extensions for this notebook (just file I/O)
try:
    from pyspark.sql import SparkSession
    # Use basic Spark session - no Delta extensions needed for file operations
    spark = SparkSession.builder \
        .appName("Data Collection") \
        .getOrCreate()
    print("Spark session initialized successfully")
except Exception as e:
    print(f"[WARN] Could not initialize Spark session: {e}")
    print("       File operations may fail. Ensure you're running in a Databricks environment.")
    spark = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Get S&P 500 Symbols

# COMMAND ----------

symbols = get_sp500_symbols()
print(f"\n=== S&P 500 Stock Symbols ({len(symbols)} total) ===")
print(f"First 10 symbols: {symbols[:10]}")
print(f"Last 10 symbols: {symbols[-10:]}")

# Save symbols list to storage
try:
    if spark is not None:
        # Create directory if it doesn't exist
        try:
            dbutils.fs.mkdirs(DATABRICKS_PATHS["raw_symbols_path"])
        except Exception as mkdir_error:
            # If mkdirs fails (e.g., Unity Catalog not set up), try creating parent directories
            print(f"[INFO] Could not create directory via mkdirs: {mkdir_error}")
            print(f"       Attempting to write directly to: {DATABRICKS_PATHS['raw_symbols_path']}")
        
        symbols_file_path = f"{DATABRICKS_PATHS['raw_symbols_path']}/sp500_symbols_{datetime.now().strftime('%Y%m%d')}.json"
        dbutils.fs.put(symbols_file_path, json.dumps(symbols, indent=2), overwrite=True)
        print(f"\n[OK] Saved symbols list to: {symbols_file_path}")
    else:
        print(f"\n[WARN] Spark session not available, skipping symbol file save")
        print("       Symbols will be saved with the bar data collection")
except Exception as e:
    print(f"\n[WARN] Could not save symbols to storage: {e}")
    print("       Continuing with data collection...")
    print("       Note: If using Unity Catalog volumes, ensure catalog 'main' and schema 'default' exist")
    print("       Or change STORAGE_MODE to 'dbfs' in src/config.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Determine Target Date

# COMMAND ----------

last_trading_day = get_last_trading_day()
print(f"\n=== Target Date ===")
print(f"Last complete trading day: {last_trading_day}")
print(f"Day of week: {['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][last_trading_day.weekday()]}")
print(f"Note: Automatically skips weekends and NYSE holidays")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Collect Market Data

# COMMAND ----------

# Fetch data for all symbols (or a subset for testing)
# In production, you might want to process in batches
# For now, we'll fetch for a sample to demonstrate the pipeline

# Uncomment the line below to fetch for ALL S&P 500 symbols (takes ~30-60 minutes)
# target_symbols = symbols

# For testing/demo, use a subset
target_symbols = symbols[:10]  # First 10 symbols for demo
print(f"\n=== Collecting Data ===")
print(f"Fetching 5-minute bar data for {len(target_symbols)} symbols...")
print(f"Symbols: {target_symbols[:5]}..." if len(target_symbols) > 5 else f"Symbols: {target_symbols}")

bars_data = fetch_previous_day_5min_bars(target_symbols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write Raw Data to Volume

# COMMAND ----------

# Create storage directory if it doesn't exist
try:
    # Ensure Spark session is available for dbutils.fs operations
    if spark is None:
        raise RuntimeError(
            "Spark session is required for writing to Databricks storage. "
            "Please ensure you're running this notebook in a Databricks environment."
        )
    
    # Try to create directories (may fail for Unity Catalog if not set up)
    try:
        dbutils.fs.mkdirs(DATABRICKS_PATHS["raw_bars_path"])
    except Exception as mkdir_error:
        print(f"[INFO] Could not create directory via mkdirs: {mkdir_error}")
        print(f"       Will attempt to write directly to: {DATABRICKS_PATHS['raw_bars_path']}")
    
    # Create date-specific subdirectory
    date_str = last_trading_day.strftime("%Y%m%d")
    date_path = f"{DATABRICKS_PATHS['raw_bars_path']}"
    try:
        dbutils.fs.mkdirs(date_path)
    except Exception:
        pass  # Directory might already exist or will be created on first write
    
    # Write all symbols' data as a single timestamped JSON file
    # This is more efficient for Spark to read and process
    all_bars_flat = []
    symbols_with_data = 0
    total_bars = 0
    
    for symbol, bars in bars_data.items():
        if bars:
            all_bars_flat.extend(bars)
            symbols_with_data += 1
            total_bars += len(bars)
            print(f"[OK] {symbol}: {len(bars)} bars")
        else:
            print(f"[WARN] {symbol}: No data available")
    
    # Write single JSON file with all data for the date
    if all_bars_flat:
        file_path = f"{date_path}/bars_{date_str}.json"
        # Convert datetime objects to strings for JSON serialization
        bars_json = json.dumps(all_bars_flat, default=str, indent=2)
        dbutils.fs.put(file_path, bars_json, overwrite=True)
        
        print(f"\n=== Collection Summary ===")
        print(f"Date: {last_trading_day} ({date_str})")
        print(f"Symbols processed: {len(bars_data)}")
        print(f"Symbols with data: {symbols_with_data}")
        print(f"Total bars collected: {total_bars}")
        print(f"File written: {file_path}")
        print(f"File size: {len(bars_json)} bytes")
    else:
        print(f"\n[WARN] No data collected for {last_trading_day}")
        print("       No file written")
    
except Exception as e:
    print(f"\n[ERROR] Failed to write data to volume: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collection Complete
# MAGIC
# MAGIC Raw data has been written to the volume. The next pipeline step (`02_ingest_bronze_bars.py`)
# MAGIC will read from this volume and load data into Bronze Delta tables.


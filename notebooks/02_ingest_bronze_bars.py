# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Ingest Raw Data to Delta Tables
# MAGIC
# MAGIC **Production Pipeline - Step 2: Bronze Ingestion**
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads raw JSON files from Databricks Volume (landing zone)
# MAGIC 2. Validates and structures the data according to Bronze schema
# MAGIC 3. Writes to Bronze Delta tables using idempotent MERGE operations
# MAGIC
# MAGIC **Idempotent**: Uses Delta MERGE to safely re-run without duplicates

# COMMAND ----------

import sys
import os
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

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
            if (notebook_dir / '02_ingest_bronze_bars.py').exists():
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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType
from src.config import DATABRICKS_PATHS
from src.schemas import BRONZE_BARS_SCHEMA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Initialize Spark Session

# COMMAND ----------

spark = SparkSession.builder \
    .appName("Bronze Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("Spark session initialized with Delta Lake support")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Read Raw Data from Volume

# COMMAND ----------

# Get the most recent timestamped file, or use a specific date
# In production, you might pass the date as a parameter or use the most recent

raw_bars_path = DATABRICKS_PATHS["raw_bars_path"]

# List available timestamped JSON files
try:
    # Find all timestamped JSON files (format: bars_YYYYMMDD.json)
    json_files = [f.path for f in dbutils.fs.ls(raw_bars_path) if f.name.startswith('bars_') and f.name.endswith('.json')]
    
    if not json_files:
        # Fallback: look for any JSON files (for backward compatibility)
        json_files = [f.path for f in dbutils.fs.ls(raw_bars_path) if f.name.endswith('.json')]
        if json_files:
            print(f"[WARN] Found {len(json_files)} JSON files, but expected timestamped files (bars_YYYYMMDD.json)")
            print(f"       Using most recent file")
        else:
            raise ValueError(f"No JSON files found in {raw_bars_path}")
    
    # Use the most recent file (extract date from filename)
    if len(json_files) > 1:
        # Sort by filename (which includes date) to get most recent
        json_files = sorted(json_files)
        print(f"[INFO] Found {len(json_files)} timestamped files, using most recent")
    
    latest_file = json_files[-1]
    # Extract date from filename (bars_YYYYMMDD.json)
    filename = Path(latest_file).name
    date_str = filename.replace('bars_', '').replace('.json', '')
    latest_date_dir = date_str
    
    print(f"Reading raw data from: {raw_bars_path}")
    print(f"Selected file: {latest_file}")
    print(f"Date: {latest_date_dir}")
    
    json_files = [latest_file]
    
except Exception as e:
    print(f"[ERROR] Failed to read from volume: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Load and Structure Data

# COMMAND ----------

# Read the single timestamped JSON file using Spark (handles large files efficiently)
file_path = json_files[0]
print(f"Reading data from: {file_path}")

try:
    # Use Spark to read JSON file (handles large files better than dbutils.fs.head)
    # Spark can read JSON files directly and handle size limits automatically
    raw_df = spark.read.json(file_path)
    
    print(f"[OK] Loaded JSON file into DataFrame")
    print(f"     Initial row count: {raw_df.count()}")
    
    # Add metadata columns
    ingestion_timestamp = datetime.now()
    from pyspark.sql.functions import lit
    
    bronze_df = raw_df \
        .withColumn("ingestion_timestamp", lit(ingestion_timestamp)) \
        .withColumn("batch_id", lit(latest_date_dir))
    
    print(f"[OK] Added metadata columns")
    print(f"     Final row count: {bronze_df.count()}")
    
    # Count unique symbols
    unique_symbols = bronze_df.select("symbol").distinct().count()
    print(f"     Contains data for {unique_symbols} symbols")
    
except Exception as e:
    print(f"[ERROR] Failed to load JSON file with Spark: {e}")
    print("       Falling back to Python JSON parsing...")
    
    # Fallback: try reading with Python (for smaller files)
    try:
        json_content = dbutils.fs.head(file_path, maxBytes=100 * 1024 * 1024)  # 100MB limit
        bars_data = json.loads(json_content)
        
        # Add metadata to each bar
        ingestion_timestamp = datetime.now()
        for bar in bars_data:
            bar["ingestion_timestamp"] = ingestion_timestamp.isoformat()
            bar["batch_id"] = latest_date_dir
        
        # Create DataFrame from Python list
        bronze_df = spark.createDataFrame(bars_data, schema=BRONZE_BARS_SCHEMA)
        print(f"[OK] Loaded {len(bars_data)} bars using Python fallback")
    except Exception as fallback_error:
        print(f"[ERROR] Fallback also failed: {fallback_error}")
        raise

if bronze_df.count() == 0:
    raise ValueError("No data loaded from volume file")

print(f"\nTotal bars loaded: {bronze_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create DataFrame and Apply Schema

# COMMAND ----------

# Ensure timestamp columns are properly typed
# (Spark JSON reader may have already converted them, but we'll ensure it)
from pyspark.sql.functions import to_timestamp

# Check if timestamp columns need conversion
if bronze_df.schema["timestamp"].dataType.typeName() != "timestamp":
    bronze_df = bronze_df.withColumn("timestamp", to_timestamp(col("timestamp")))

if bronze_df.schema["ingestion_timestamp"].dataType.typeName() != "timestamp":
    bronze_df = bronze_df.withColumn("ingestion_timestamp", to_timestamp(col("ingestion_timestamp")))

# Ensure schema matches BRONZE_BARS_SCHEMA (Spark may infer slightly different types)
# Cast to ensure consistency
bronze_df = bronze_df.select(
    col("symbol").cast("string"),
    col("timestamp").cast("timestamp"),
    col("open").cast("double"),
    col("high").cast("double"),
    col("low").cast("double"),
    col("close").cast("double"),
    col("volume").cast("bigint"),
    col("ingestion_timestamp").cast("timestamp"),
    col("batch_id").cast("string")
)

print(f"Bronze DataFrame prepared with {bronze_df.count()} rows")
bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Write to Bronze Delta Table (Idempotent MERGE)

# COMMAND ----------

bronze_table = DATABRICKS_PATHS["bronze_bars_table"]

# Create catalog and schema if they don't exist
catalog = DATABRICKS_PATHS["bronze_catalog"]
schema = DATABRICKS_PATHS["bronze_schema"]

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    print(f"Created catalog/schema: {catalog}.{schema}")
except Exception as e:
    print(f"Note: {e}")

# Write to Delta table using MERGE for idempotency
# This ensures we can re-run the ingestion without creating duplicates
try:
    # Check if table exists
    table_exists = spark.catalog.tableExists(bronze_table)
    
    if table_exists:
        # Use MERGE to upsert data (idempotent)
        print(f"Table {bronze_table} exists. Using MERGE for idempotent upsert...")
        
        # Create temporary view for merge
        bronze_df.createOrReplaceTempView("new_bronze_data")
        
        # MERGE statement: update if exists, insert if not
        merge_sql = f"""
        MERGE INTO {bronze_table} AS target
        USING new_bronze_data AS source
        ON target.symbol = source.symbol 
           AND target.timestamp = source.timestamp
           AND target.batch_id = source.batch_id
        WHEN MATCHED THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
        """
        
        spark.sql(merge_sql)
        print(f"[OK] Merged data into {bronze_table}")
        
    else:
        # First time: create table
        print(f"Table {bronze_table} does not exist. Creating new table...")
        bronze_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(bronze_table)
        print(f"[OK] Created and wrote to {bronze_table}")
    
    # Verify write
    final_count = spark.table(bronze_table).count()
    print(f"\n=== Ingestion Complete ===")
    print(f"Total rows in {bronze_table}: {final_count}")
    
except Exception as e:
    print(f"[ERROR] Failed to write to Delta table: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Complete
# MAGIC
# MAGIC Data has been successfully ingested into the Bronze Delta table.
# MAGIC The next pipeline step (`03_transform_silver_bars.py`) will read from Bronze
# MAGIC and transform data into the Silver layer.


"""
Spark schemas for market data tables.

TODO: Define explicit Spark schemas for:
- Bronze layer: Raw ingested data schema
- Silver layer: Cleaned/normalized data schema
- Gold layer: Analytics/aggregated data schema

Example structure:
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

BRONZE_BARS_SCHEMA = StructType([
    StructField("symbol", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    # TODO: Add more fields
])
"""


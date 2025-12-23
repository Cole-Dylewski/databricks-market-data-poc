# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC TODO: Implement bronze layer ingestion:
# MAGIC - Import data source clients
# MAGIC - Fetch data from configured sources (Yahoo Finance, Alpaca, etc.)
# MAGIC - Add metadata (ingestion timestamp, data source name)
# MAGIC - Write raw data to bronze Delta tables (append-only)
# MAGIC - Handle errors and retries

# TODO: Add ingestion code
# Example:
# from src.data_sources.yahoo_finance import YahooFinanceClient
# from src.schemas import BRONZE_BARS_SCHEMA
# from datetime import datetime, timedelta
#
# # Initialize data source client
# client = YahooFinanceClient()
#
# # Fetch data
# symbols = ["AAPL", "MSFT", "GOOGL"]  # TODO: Load from config
# end_time = datetime.now()
# start_time = end_time - timedelta(days=7)  # TODO: Make configurable
#
# # Fetch and write to bronze
# for symbol in symbols:
#     bars = client.fetch_bars(symbol, start_time, end_time)
#     # TODO: Write to bronze Delta table


"""
Yahoo Finance data source client.

Uses yfinance library to fetch historical stock market data.
No API keys required - uses public Yahoo Finance data.
"""

from datetime import datetime
from typing import List, Dict, Any, Optional

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    raise ImportError(
        "yfinance is required but not installed. "
        "Install it with: pip install yfinance>=0.2.0 "
        "or in Databricks: %pip install yfinance>=0.2.0"
    )

# Verify yfinance is being used (not requests directly)
if not hasattr(yf, "Ticker"):
    raise ImportError(
        "yfinance library is not properly installed or is the wrong version. "
        "Expected yfinance>=0.2.0 with Ticker class."
    )

from .base_client import BaseMarketDataClient


class YahooFinanceClient(BaseMarketDataClient):
    """Client for fetching data from Yahoo Finance using yfinance library.

    Uses the yfinance library which wraps Yahoo Finance's public API.
    More reliable than direct REST API calls and handles rate limiting better.
    No API keys required.
    """

    def __init__(self) -> None:
        """Initialize Yahoo Finance client.

        Examples:
            >>> client = YahooFinanceClient()
        """
        pass

    def _convert_interval(self, interval: str) -> str:
        """Convert standard interval to yfinance format.

        Args:
            interval: Standard interval (e.g., "1d", "1h", "5m")

        Returns:
            yfinance compatible interval string

        Examples:
            >>> client = YahooFinanceClient()
            >>> client._convert_interval("1d")
            '1d'
            >>> client._convert_interval("5m")
            '5m'
        """
        # yfinance supports: 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo
        valid_intervals = [
            "1m",
            "2m",
            "5m",
            "15m",
            "30m",
            "60m",
            "90m",
            "1h",
            "1d",
            "5d",
            "1wk",
            "1mo",
            "3mo",
        ]
        if interval not in valid_intervals:
            return "1d"
        return interval

    def fetch_bars(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "1d",
    ) -> List[Dict[str, Any]]:
        """Fetch bar data from Yahoo Finance using yfinance library only.

        This method ONLY uses yfinance library - no direct REST API calls.
        yfinance handles all HTTP requests internally.

        Args:
            symbol: Stock symbol (e.g., "AAPL", "MSFT")
            start_time: Start datetime for data range
            end_time: End datetime for data range
            interval: Bar interval (default: "1d")

        Returns:
            List of bar dictionaries with OHLCV data

        Raises:
            ValueError: If symbol is empty or time range is invalid
            ConnectionError: If API request fails

        Examples:
            >>> client = YahooFinanceClient()
            >>> start = datetime(2024, 1, 1)
            >>> end = datetime(2024, 1, 31)
            >>> bars = client.fetch_bars("AAPL", start, end, "1d")
            >>> len(bars) > 0
            True
            >>> "symbol" in bars[0] and "timestamp" in bars[0]
            True
        """
        if not symbol or not symbol.strip():
            raise ValueError("Symbol cannot be empty")

        if start_time >= end_time:
            raise ValueError("start_time must be before end_time")

        yfinance_interval = self._convert_interval(interval)

        try:
            # ONLY use yfinance - no REST API calls
            # yfinance.Ticker() creates a ticker object
            # ticker.history() downloads data using yfinance's internal HTTP handling
            ticker = yf.Ticker(symbol)

            # Fetch historical data using yfinance
            # yfinance handles all HTTP requests, retries, and rate limiting internally
            # This is the ONLY way we fetch data - no direct REST API calls
            df = ticker.history(
                start=start_time,
                end=end_time,
                interval=yfinance_interval,
            )

            # Convert pandas DataFrame to list of dictionaries
            return self._dataframe_to_bars(symbol, df)

        except Exception as e:
            error_msg = str(e)
            # Provide clearer error messages
            if "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
                raise ConnectionError(
                    f"Timeout fetching data for {symbol} from Yahoo Finance via yfinance. "
                    f"Error: {error_msg}"
                ) from e
            raise ConnectionError(
                f"Failed to fetch data for {symbol} from Yahoo Finance via yfinance: {error_msg}"
            ) from e

    def _dataframe_to_bars(self, symbol: str, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Convert pandas DataFrame from yfinance to standardized bar format.

        Args:
            symbol: Stock symbol
            df: pandas DataFrame from yfinance with OHLCV data

        Returns:
            List of standardized bar dictionaries
        """
        if df.empty:
            return []

        bars = []
        for timestamp, row in df.iterrows():
            # Handle pandas Timestamp
            if isinstance(timestamp, pd.Timestamp):
                bar_timestamp = timestamp.to_pydatetime()
            else:
                bar_timestamp = datetime.fromtimestamp(timestamp.timestamp())

            bars.append(
                {
                    "symbol": symbol,
                    "timestamp": bar_timestamp,
                    "open": float(row["Open"]) if pd.notna(row["Open"]) else 0.0,
                    "high": float(row["High"]) if pd.notna(row["High"]) else 0.0,
                    "low": float(row["Low"]) if pd.notna(row["Low"]) else 0.0,
                    "close": float(row["Close"]) if pd.notna(row["Close"]) else 0.0,
                    "volume": int(row["Volume"]) if pd.notna(row["Volume"]) else 0,
                }
            )

        return bars

    def get_available_symbols(self) -> List[str]:
        """Get list of available symbols.

        Note: Yahoo Finance doesn't provide a public endpoint for this.
        This method returns common symbols as a fallback.

        Returns:
            List of common stock symbols

        Examples:
            >>> client = YahooFinanceClient()
            >>> symbols = client.get_available_symbols()
            >>> "AAPL" in symbols
            True
        """
        return [
            "AAPL",
            "MSFT",
            "GOOGL",
            "AMZN",
            "META",
            "TSLA",
            "NVDA",
            "JPM",
            "V",
            "JNJ",
            "WMT",
            "PG",
            "MA",
            "UNH",
            "HD",
            "DIS",
            "BAC",
            "ADBE",
            "NFLX",
            "CRM",
            "PYPL",
            "INTC",
        ]

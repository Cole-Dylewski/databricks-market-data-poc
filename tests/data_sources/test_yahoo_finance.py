"""
Tests for Yahoo Finance client.
"""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any
import pandas as pd

from src.data_sources.yahoo_finance import YahooFinanceClient


class TestYahooFinanceClient:
    """Test suite for YahooFinanceClient."""

    def test_init(self) -> None:
        """Test client initialization."""
        client = YahooFinanceClient()
        assert client is not None

    def test_convert_interval_valid(self) -> None:
        """Test interval conversion with valid intervals."""
        client = YahooFinanceClient()
        assert client._convert_interval("1d") == "1d"
        assert client._convert_interval("1h") == "1h"
        assert client._convert_interval("5m") == "5m"
        assert client._convert_interval("15m") == "15m"

    def test_convert_interval_invalid(self) -> None:
        """Test interval conversion with invalid interval defaults to 1d."""
        client = YahooFinanceClient()
        assert client._convert_interval("invalid") == "1d"
        assert client._convert_interval("") == "1d"

    def test_fetch_bars_empty_symbol(self) -> None:
        """Test fetch_bars raises ValueError for empty symbol."""
        client = YahooFinanceClient()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)

        with pytest.raises(ValueError, match="Symbol cannot be empty"):
            client.fetch_bars("", start, end)

        with pytest.raises(ValueError, match="Symbol cannot be empty"):
            client.fetch_bars("   ", start, end)

    def test_fetch_bars_invalid_time_range(self) -> None:
        """Test fetch_bars raises ValueError for invalid time range."""
        client = YahooFinanceClient()
        start = datetime(2024, 1, 31)
        end = datetime(2024, 1, 1)

        with pytest.raises(ValueError, match="start_time must be before end_time"):
            client.fetch_bars("AAPL", start, end)

        with pytest.raises(ValueError, match="start_time must be before end_time"):
            client.fetch_bars("AAPL", start, start)

    @patch("src.data_sources.yahoo_finance.yf.Ticker")
    def test_fetch_bars_success(self, mock_ticker_class: Mock) -> None:
        """Test successful fetch_bars call."""
        # Create mock DataFrame
        dates = pd.date_range("2024-01-01", periods=2, freq="D")
        mock_df = pd.DataFrame(
            {
                "Open": [150.0, 151.0],
                "High": [152.0, 153.0],
                "Low": [149.0, 150.0],
                "Close": [151.0, 152.0],
                "Volume": [1000000, 1100000],
            },
            index=dates,
        )

        mock_ticker = Mock()
        mock_ticker.history.return_value = mock_df
        mock_ticker_class.return_value = mock_ticker

        client = YahooFinanceClient()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)

        bars = client.fetch_bars("AAPL", start, end, "1d")

        assert len(bars) == 2
        assert bars[0]["symbol"] == "AAPL"
        assert bars[0]["open"] == 150.0
        assert bars[0]["high"] == 152.0
        assert bars[0]["low"] == 149.0
        assert bars[0]["close"] == 151.0
        assert bars[0]["volume"] == 1000000
        assert isinstance(bars[0]["timestamp"], datetime)

        # Verify yfinance was called correctly
        mock_ticker_class.assert_called_once_with("AAPL")
        mock_ticker.history.assert_called_once_with(start=start, end=end, interval="1d")

    @patch("src.data_sources.yahoo_finance.yf.Ticker")
    def test_fetch_bars_empty_dataframe(self, mock_ticker_class: Mock) -> None:
        """Test fetch_bars handles empty DataFrame."""
        mock_ticker = Mock()
        mock_ticker.history.return_value = pd.DataFrame()
        mock_ticker_class.return_value = mock_ticker

        client = YahooFinanceClient()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)

        bars = client.fetch_bars("AAPL", start, end)
        assert bars == []

    @patch("src.data_sources.yahoo_finance.yf.Ticker")
    def test_fetch_bars_api_error(self, mock_ticker_class: Mock) -> None:
        """Test fetch_bars handles API errors."""
        mock_ticker = Mock()
        mock_ticker.history.side_effect = Exception("Network error")
        mock_ticker_class.return_value = mock_ticker

        client = YahooFinanceClient()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)

        with pytest.raises(ConnectionError, match="Failed to fetch data"):
            client.fetch_bars("AAPL", start, end)

    @patch("src.data_sources.yahoo_finance.yf.Ticker")
    def test_fetch_bars_5min_interval(self, mock_ticker_class: Mock) -> None:
        """Test fetch_bars with 5-minute interval."""
        # Create mock DataFrame with 5-minute data
        dates = pd.date_range("2024-01-01 09:30", periods=3, freq="5min")
        mock_df = pd.DataFrame(
            {
                "Open": [150.0, 150.5, 151.0],
                "High": [150.5, 151.0, 151.5],
                "Low": [149.5, 150.0, 150.5],
                "Close": [150.5, 151.0, 151.5],
                "Volume": [100000, 110000, 120000],
            },
            index=dates,
        )

        mock_ticker = Mock()
        mock_ticker.history.return_value = mock_df
        mock_ticker_class.return_value = mock_ticker

        client = YahooFinanceClient()
        start = datetime(2024, 1, 1, 9, 30)
        end = datetime(2024, 1, 1, 16, 0)

        bars = client.fetch_bars("AAPL", start, end, "5m")

        assert len(bars) == 3
        assert bars[0]["symbol"] == "AAPL"
        assert bars[0]["close"] == 150.5
        assert mock_ticker.history.call_args[1]["interval"] == "5m"

    def test_dataframe_to_bars(self) -> None:
        """Test _dataframe_to_bars conversion."""
        client = YahooFinanceClient()

        dates = pd.date_range("2024-01-01", periods=2, freq="D")
        df = pd.DataFrame(
            {
                "Open": [150.0, 151.0],
                "High": [152.0, 153.0],
                "Low": [149.0, 150.0],
                "Close": [151.0, 152.0],
                "Volume": [1000000, 1100000],
            },
            index=dates,
        )

        bars = client._dataframe_to_bars("AAPL", df)

        assert len(bars) == 2
        assert bars[0]["symbol"] == "AAPL"
        assert bars[0]["open"] == 150.0
        assert bars[0]["high"] == 152.0
        assert bars[0]["low"] == 149.0
        assert bars[0]["close"] == 151.0
        assert bars[0]["volume"] == 1000000
        assert isinstance(bars[0]["timestamp"], datetime)

    def test_dataframe_to_bars_empty(self) -> None:
        """Test _dataframe_to_bars with empty DataFrame."""
        client = YahooFinanceClient()
        df = pd.DataFrame()

        bars = client._dataframe_to_bars("AAPL", df)
        assert bars == []

    def test_dataframe_to_bars_missing_values(self) -> None:
        """Test _dataframe_to_bars handles missing values."""
        client = YahooFinanceClient()

        dates = pd.date_range("2024-01-01", periods=1, freq="D")
        df = pd.DataFrame(
            {
                "Open": [None],
                "High": [None],
                "Low": [None],
                "Close": [150.0],
                "Volume": [None],
            },
            index=dates,
        )

        bars = client._dataframe_to_bars("AAPL", df)

        assert len(bars) == 1
        assert bars[0]["open"] == 0.0
        assert bars[0]["high"] == 0.0
        assert bars[0]["low"] == 0.0
        assert bars[0]["close"] == 150.0
        assert bars[0]["volume"] == 0

    def test_get_available_symbols(self) -> None:
        """Test get_available_symbols returns list of symbols."""
        client = YahooFinanceClient()
        symbols = client.get_available_symbols()

        assert isinstance(symbols, list)
        assert len(symbols) > 0
        assert "AAPL" in symbols
        assert "MSFT" in symbols
        assert all(isinstance(s, str) for s in symbols)

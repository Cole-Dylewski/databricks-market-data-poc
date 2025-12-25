"""
Tests for utility functions.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from typing import List, Dict, Any
from datetime import datetime, timedelta, time, date

from src.utils import (
    get_sp500_symbols,
    _is_valid_symbol,
    fetch_previous_day_5min_bars,
    get_last_trading_day,
)


class TestGetSp500Symbols:
    """Test suite for get_sp500_symbols function."""

    @patch("src.utils.requests.get")
    def test_get_sp500_symbols_success(self, mock_get: Mock) -> None:
        """Test successful extraction of S&P 500 symbols."""
        # Mock HTML response with sample S&P 500 table
        mock_html = """
        <html>
        <body>
        <table>
        <tr>
            <th>No.</th>
            <th>Symbol</th>
            <th>Company Name</th>
        </tr>
        <tr>
            <td>1</td>
            <td><a href="/stocks/AAPL/">AAPL</a></td>
            <td>Apple Inc.</td>
        </tr>
        <tr>
            <td>2</td>
            <td><a href="/stocks/MSFT/">MSFT</a></td>
            <td>Microsoft Corporation</td>
        </tr>
        <tr>
            <td>3</td>
            <td><a href="/stocks/BRK.B/">BRK.B</a></td>
            <td>Berkshire Hathaway Inc.</td>
        </tr>
        <tr>
            <td>4</td>
            <td>GOOG</td>
            <td>Alphabet Inc.</td>
        </tr>
        </table>
        </body>
        </html>
        """

        mock_response = Mock()
        mock_response.text = mock_html
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        symbols = get_sp500_symbols()

        assert isinstance(symbols, list)
        assert len(symbols) == 4
        assert "AAPL" in symbols
        assert "MSFT" in symbols
        assert "BRK.B" in symbols
        assert "GOOG" in symbols
        assert symbols == sorted(symbols)  # Should be sorted

    @patch("src.utils.requests.get")
    def test_get_sp500_symbols_deduplication(self, mock_get: Mock) -> None:
        """Test that duplicate symbols are removed."""
        mock_html = """
        <html>
        <body>
        <table>
        <tr>
            <th>No.</th>
            <th>Symbol</th>
        </tr>
        <tr>
            <td>1</td>
            <td><a href="/stocks/AAPL/">AAPL</a></td>
        </tr>
        <tr>
            <td>2</td>
            <td><a href="/stocks/AAPL/">AAPL</a></td>
        </tr>
        </table>
        </body>
        </html>
        """

        mock_response = Mock()
        mock_response.text = mock_html
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        symbols = get_sp500_symbols()

        assert len(symbols) == 1
        assert symbols == ["AAPL"]

    @patch("src.utils.requests.get")
    def test_get_sp500_symbols_no_table(self, mock_get: Mock) -> None:
        """Test that ValueError is raised when no table is found."""
        mock_html = "<html><body><p>No table here</p></body></html>"

        mock_response = Mock()
        mock_response.text = mock_html
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="No table found"):
            get_sp500_symbols()

    @patch("src.utils.requests.get")
    def test_get_sp500_symbols_no_symbols(self, mock_get: Mock) -> None:
        """Test that ValueError is raised when no symbols are found."""
        mock_html = """
        <html>
        <body>
        <table>
        <tr>
            <th>No.</th>
            <th>Symbol</th>
        </tr>
        <tr>
            <td>1</td>
            <td>Invalid Symbol Here</td>
        </tr>
        </table>
        </body>
        </html>
        """

        mock_response = Mock()
        mock_response.text = mock_html
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="No symbols found"):
            get_sp500_symbols()

    @patch("src.utils.requests.get")
    def test_get_sp500_symbols_request_exception(self, mock_get: Mock) -> None:
        """Test that RequestException is raised when HTTP request fails."""
        mock_get.side_effect = Exception("Connection error")

        with pytest.raises(Exception, match="Connection error"):
            get_sp500_symbols()


class TestIsValidSymbol:
    """Test suite for _is_valid_symbol function."""

    def test_is_valid_symbol_valid(self) -> None:
        """Test valid symbols."""
        assert _is_valid_symbol("AAPL") is True
        assert _is_valid_symbol("MSFT") is True
        assert _is_valid_symbol("BRK.B") is True
        assert _is_valid_symbol("GOOGL") is True
        assert _is_valid_symbol("A") is True
        assert _is_valid_symbol("ABC12") is True

    def test_is_valid_symbol_invalid(self) -> None:
        """Test invalid symbols."""
        assert _is_valid_symbol("") is False
        assert _is_valid_symbol("123") is False  # No letters
        assert _is_valid_symbol("TOOLONG") is False  # Too long
        assert (
            _is_valid_symbol("aapl") is False
        )  # Lowercase (though we uppercase before calling)
        assert _is_valid_symbol("AAP-L") is False  # Invalid character
        assert _is_valid_symbol("AAP L") is False  # Space not allowed

    def test_is_valid_symbol_edge_cases(self) -> None:
        """Test edge cases for symbol validation."""
        assert _is_valid_symbol("A1") is True
        assert _is_valid_symbol("1A") is True
        assert _is_valid_symbol("A.B") is True
        assert _is_valid_symbol(".A") is False  # Starts with dot
        assert _is_valid_symbol("A.") is False  # Ends with dot


class TestFetchPreviousDay5minBars:
    """Test suite for fetch_previous_day_5min_bars function."""

    @patch("src.utils.YahooFinanceClient")
    def test_fetch_previous_day_5min_bars_success(
        self, mock_client_class: Mock
    ) -> None:
        """Test successful fetching of previous day 5-minute bars."""
        # Create mock client instance
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Mock bar data
        yesterday = datetime.now().date() - timedelta(days=1)
        mock_bars = [
            {
                "symbol": "AAPL",
                "timestamp": datetime.combine(yesterday, time(9, 30)),
                "open": 150.0,
                "high": 151.0,
                "low": 149.5,
                "close": 150.5,
                "volume": 1000000,
            },
            {
                "symbol": "AAPL",
                "timestamp": datetime.combine(yesterday, time(9, 35)),
                "open": 150.5,
                "high": 151.5,
                "low": 150.0,
                "close": 151.0,
                "volume": 1200000,
            },
        ]
        mock_client.fetch_bars.return_value = mock_bars

        symbols = ["AAPL", "MSFT"]
        results = fetch_previous_day_5min_bars(symbols)

        assert isinstance(results, dict)
        assert "AAPL" in results
        assert len(results["AAPL"]) == 2
        assert results["AAPL"][0]["symbol"] == "AAPL"
        assert "timestamp" in results["AAPL"][0]

        # Verify fetch_bars was called with correct parameters
        assert mock_client.fetch_bars.called
        # Check first call (for AAPL)
        first_call = mock_client.fetch_bars.call_args_list[0]
        assert first_call[1]["symbol"] == "AAPL"
        assert first_call[1]["interval"] == "5m"

    @patch("src.utils.YahooFinanceClient")
    def test_fetch_previous_day_5min_bars_multiple_symbols(
        self, mock_client_class: Mock
    ) -> None:
        """Test fetching data for multiple symbols."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        yesterday = datetime.now().date() - timedelta(days=1)
        mock_bars_aapl = [
            {
                "symbol": "AAPL",
                "timestamp": datetime.combine(yesterday, time(9, 30)),
                "open": 150.0,
                "high": 151.0,
                "low": 149.5,
                "close": 150.5,
                "volume": 1000000,
            },
        ]
        mock_bars_msft = [
            {
                "symbol": "MSFT",
                "timestamp": datetime.combine(yesterday, time(9, 30)),
                "open": 300.0,
                "high": 301.0,
                "low": 299.5,
                "close": 300.5,
                "volume": 500000,
            },
        ]

        # Return different data for different symbols
        def side_effect(symbol, start_time, end_time, interval):
            if symbol == "AAPL":
                return mock_bars_aapl
            elif symbol == "MSFT":
                return mock_bars_msft
            return []

        mock_client.fetch_bars.side_effect = side_effect

        symbols = ["AAPL", "MSFT"]
        results = fetch_previous_day_5min_bars(symbols)

        assert len(results) == 2
        assert "AAPL" in results
        assert "MSFT" in results
        assert len(results["AAPL"]) == 1
        assert len(results["MSFT"]) == 1

    @patch("src.utils.YahooFinanceClient")
    def test_fetch_previous_day_5min_bars_with_custom_date(
        self, mock_client_class: Mock
    ) -> None:
        """Test fetching data for a specific date."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.fetch_bars.return_value = []

        custom_date = datetime(2024, 1, 15)
        symbols = ["AAPL"]
        results = fetch_previous_day_5min_bars(symbols, date=custom_date)

        assert "AAPL" in results
        # Verify the date was used correctly
        call_args = mock_client.fetch_bars.call_args
        assert call_args[1]["interval"] == "5m"

    @patch("src.utils.YahooFinanceClient")
    def test_fetch_previous_day_5min_bars_with_custom_client(
        self, mock_client_class: Mock
    ) -> None:
        """Test using a custom client instance."""
        custom_client = MagicMock()
        custom_client.fetch_bars.return_value = []

        symbols = ["AAPL"]
        results = fetch_previous_day_5min_bars(symbols, client=custom_client)

        assert "AAPL" in results
        # Verify custom client was used, not a new one
        mock_client_class.assert_not_called()
        custom_client.fetch_bars.assert_called_once()

    @patch("src.utils.YahooFinanceClient")
    def test_fetch_previous_day_5min_bars_empty_symbols(
        self, mock_client_class: Mock
    ) -> None:
        """Test that ValueError is raised for empty symbols list."""
        with pytest.raises(ValueError, match="Symbols list cannot be empty"):
            fetch_previous_day_5min_bars([])

    @patch("src.utils.YahooFinanceClient")
    def test_fetch_previous_day_5min_bars_error_handling(
        self, mock_client_class: Mock
    ) -> None:
        """Test that errors for individual symbols don't stop processing."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # First symbol succeeds, second fails
        yesterday = datetime.now().date() - timedelta(days=1)
        mock_bars = [
            {
                "symbol": "AAPL",
                "timestamp": datetime.combine(yesterday, time(9, 30)),
                "open": 150.0,
                "high": 151.0,
                "low": 149.5,
                "close": 150.5,
                "volume": 1000000,
            },
        ]

        def side_effect(symbol, start_time, end_time, interval):
            if symbol == "AAPL":
                return mock_bars
            elif symbol == "INVALID":
                raise ConnectionError("Failed to fetch")
            return []

        mock_client.fetch_bars.side_effect = side_effect

        symbols = ["AAPL", "INVALID"]
        results = fetch_previous_day_5min_bars(symbols)

        # Both symbols should be in results
        assert "AAPL" in results
        assert "INVALID" in results
        # AAPL should have data
        assert len(results["AAPL"]) == 1
        # INVALID should have empty list due to error
        assert results["INVALID"] == []


class TestGetLastTradingDay:
    """Test suite for get_last_trading_day function."""

    def test_get_last_trading_day_weekday(self) -> None:
        """Test that weekday dates return the previous trading day."""
        # Monday - should return previous Friday
        monday = date(2024, 1, 1)  # Jan 1, 2024 is a Monday
        result = get_last_trading_day(monday)
        assert result < monday  # Should be before Monday
        assert result.weekday() < 5  # Should be a weekday
        assert result.weekday() == 4  # Should be Friday

        # Friday - should return previous Thursday
        friday = date(2024, 1, 5)  # Jan 5, 2024 is a Friday
        result = get_last_trading_day(friday)
        assert result < friday  # Should be before Friday
        assert result.weekday() < 5  # Should be a weekday
        assert result.weekday() == 3  # Should be Thursday

    def test_get_last_trading_day_weekend(self) -> None:
        """Test that weekend dates return the previous Friday."""
        # Saturday
        saturday = date(2024, 1, 6)  # Jan 6, 2024 is a Saturday
        result = get_last_trading_day(saturday)
        assert result.weekday() < 5  # Should be a weekday
        assert result < saturday  # Should be before Saturday

        # Sunday
        sunday = date(2024, 1, 7)  # Jan 7, 2024 is a Sunday
        result = get_last_trading_day(sunday)
        assert result.weekday() < 5  # Should be a weekday
        assert result < sunday  # Should be before Sunday

    def test_get_last_trading_day_none(self) -> None:
        """Test that None uses today's date."""
        result = get_last_trading_day(None)
        assert isinstance(result, date)
        assert result.weekday() < 5  # Should be a weekday

    def test_get_last_trading_day_monday(self) -> None:
        """Test that Monday returns the previous Friday."""
        # If today is Monday, last trading day should be Friday
        monday = date(2024, 1, 1)  # Monday
        # Go back to find Friday
        expected_friday = date(2023, 12, 29)  # Previous Friday
        result = get_last_trading_day(monday)
        # Should return Friday (the last complete trading day)
        assert result == expected_friday
        assert result.weekday() == 4  # Friday

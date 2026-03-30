import pandas as pd
import numpy as np
from datetime import date

from src.fetcher import safe_get, _col_index_map, _yf_row_name, _financial_fields, _build_row
from src.models import BalanceSheetAnnual, IncomeStatementAnnual, CashFlowAnnual


class TestSafeGet:
    def test_valid_value(self):
        df = pd.DataFrame({"2024-01-01": [100.0, 200.0]}, index=["Total Assets", "Net Income"])
        assert safe_get(df, "Total Assets", 0) == 100.0

    def test_returns_float(self):
        df = pd.DataFrame({"2024-01-01": [np.int64(42)]}, index=["Row"])
        result = safe_get(df, "Row", 0)
        assert result == 42.0
        assert isinstance(result, float)

    def test_missing_row_returns_default(self):
        df = pd.DataFrame({"2024-01-01": [100.0]}, index=["Total Assets"])
        assert safe_get(df, "Missing Row", 0) is None

    def test_nan_returns_default(self):
        df = pd.DataFrame({"2024-01-01": [float("nan")]}, index=["Total Assets"])
        assert safe_get(df, "Total Assets", 0) is None

    def test_none_df_returns_default(self):
        assert safe_get(None, "Total Assets", 0) is None

    def test_empty_df_returns_default(self):
        assert safe_get(pd.DataFrame(), "Total Assets", 0) is None

    def test_none_col_index_returns_default(self):
        df = pd.DataFrame({"2024-01-01": [100.0]}, index=["Total Assets"])
        assert safe_get(df, "Total Assets", None) is None

    def test_col_index_out_of_range(self):
        df = pd.DataFrame({"2024-01-01": [100.0]}, index=["Total Assets"])
        assert safe_get(df, "Total Assets", 5) is None

    def test_custom_default(self):
        assert safe_get(None, "X", 0, default=42) == 42


class TestColIndexMap:
    def test_maps_dates_to_positions(self):
        dates = pd.to_datetime(["2024-01-01", "2023-01-01"])
        df = pd.DataFrame([[1, 2], [3, 4]], columns=dates, index=["A", "B"])
        result = _col_index_map(df)
        assert result == {date(2024, 1, 1): 0, date(2023, 1, 1): 1}

    def test_none_returns_empty(self):
        assert _col_index_map(None) == {}

    def test_empty_df_returns_empty(self):
        assert _col_index_map(pd.DataFrame()) == {}


class TestYfRowName:
    def test_simple_title_case(self):
        assert _yf_row_name("total_assets") == "Total Assets"
        assert _yf_row_name("retained_earnings") == "Retained Earnings"

    def test_acronym_overrides(self):
        assert _yf_row_name("ebitda") == "EBITDA"
        assert _yf_row_name("ebit") == "EBIT"
        assert _yf_row_name("net_ppe") == "Net PPE"
        assert _yf_row_name("gross_ppe") == "Gross PPE"
        assert _yf_row_name("diluted_eps") == "Diluted EPS"
        assert _yf_row_name("basic_eps") == "Basic EPS"

    def test_yfinance_quirk_overrides(self):
        assert _yf_row_name("cash") == "Cash And Cash Equivalents"
        assert _yf_row_name("total_liabilities") == "Total Liabilities Net Minority Interest"
        assert _yf_row_name("other_g_and_a") == "Other Gand A"


class TestFinancialFields:
    def test_excludes_ticker_and_date(self):
        fields = _financial_fields(BalanceSheetAnnual)
        assert "ticker" not in fields
        assert "fiscal_year" not in fields

    def test_includes_financial_fields(self):
        fields = _financial_fields(BalanceSheetAnnual)
        assert "total_assets" in fields
        assert "net_debt" in fields
        assert "cash" in fields

    def test_works_for_all_statement_types(self):
        assert "total_revenue" in _financial_fields(IncomeStatementAnnual)
        assert "free_cash_flow" in _financial_fields(CashFlowAnnual)


class TestBuildRow:
    def test_builds_model_from_df(self):
        dates = pd.to_datetime(["2024-06-30"])
        bs = pd.DataFrame([[1e9]], columns=dates, index=["Total Assets"])
        row = _build_row(bs, 0, "AAPL", BalanceSheetAnnual, "fiscal_year", date(2024, 6, 30))
        assert row.ticker == "AAPL"
        assert row.fiscal_year == date(2024, 6, 30)
        assert row.total_assets == 1e9
        assert row.net_debt is None  # not in the df

    def test_handles_none_df(self):
        row = _build_row(None, 0, "AAPL", BalanceSheetAnnual, "fiscal_year", date(2024, 6, 30))
        assert row.ticker == "AAPL"
        assert row.total_assets is None

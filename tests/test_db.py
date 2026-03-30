from datetime import date

from src.models import (
    Company, BalanceSheetAnnual, IncomeStatementAnnual, CashFlowAnnual,
)
from src.db import _serialize_financial_rows, _build_rpc_payload, FINANCIAL_TABLES


class TestSerializeFinancialRows:
    def test_strips_ticker(self):
        row = BalanceSheetAnnual(ticker="AAPL", fiscal_year=date(2024, 9, 28), total_assets=1e9)
        result = _serialize_financial_rows([row])
        assert len(result) == 1
        assert "ticker" not in result[0]
        assert result[0]["fiscal_year"] == "2024-09-28"
        assert result[0]["total_assets"] == 1e9

    def test_empty_list(self):
        assert _serialize_financial_rows([]) == []

    def test_multiple_rows(self):
        rows = [
            BalanceSheetAnnual(ticker="AAPL", fiscal_year=date(2024, 9, 28)),
            BalanceSheetAnnual(ticker="AAPL", fiscal_year=date(2023, 9, 30)),
        ]
        result = _serialize_financial_rows(rows)
        assert len(result) == 2
        assert all("ticker" not in r for r in result)


class TestBuildRpcPayload:
    def _make_result(self, **overrides):
        base = {
            "company": Company(ticker="AAPL", name="Apple Inc.", cik="0000320193"),
            "balance_sheet_annual": [],
            "income_statement_annual": [],
            "cash_flow_annual": [],
            "balance_sheet_quarterly": [],
            "income_statement_quarterly": [],
            "cash_flow_quarterly": [],
        }
        base.update(overrides)
        return base

    def test_includes_company_fields(self):
        payload = _build_rpc_payload(self._make_result())
        assert payload["ticker"] == "AAPL"
        assert payload["name"] == "Apple Inc."
        assert payload["cik"] == "0000320193"

    def test_includes_all_financial_tables(self):
        payload = _build_rpc_payload(self._make_result())
        for table in FINANCIAL_TABLES:
            assert table in payload
            assert isinstance(payload[table], list)

    def test_serializes_financial_rows(self):
        bs = [BalanceSheetAnnual(ticker="AAPL", fiscal_year=date(2024, 9, 28), total_assets=3.5e11)]
        payload = _build_rpc_payload(self._make_result(balance_sheet_annual=bs))
        assert len(payload["balance_sheet_annual"]) == 1
        assert "ticker" not in payload["balance_sheet_annual"][0]
        assert payload["balance_sheet_annual"][0]["total_assets"] == 3.5e11

    def test_optional_company_fields_default_none(self):
        payload = _build_rpc_payload(self._make_result())
        assert payload["sector"] is None
        assert payload["industry"] is None

    def test_company_metadata_passed_through(self):
        company = Company(
            ticker="MSFT", name="Microsoft", cik="0000789019",
            sector="Technology", industry="Software", country="US",
        )
        payload = _build_rpc_payload(self._make_result(company=company))
        assert payload["sector"] == "Technology"
        assert payload["industry"] == "Software"
        assert payload["country"] == "US"

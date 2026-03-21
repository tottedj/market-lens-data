import logging
import time
import pandas as pd
import yfinance as yf
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor

from models import (
    Company,
    BalanceSheetAnnual, IncomeStatementAnnual, CashFlowAnnual,
    BalanceSheetQuarterly, IncomeStatementQuarterly, CashFlowQuarterly,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
WORKERS = 5
DELAY_BETWEEN_BATCHES = 2  # seconds


def safe_get(df, row_name, col_index, default=None):
    if df is None or df.empty:
        return default
    if row_name in df.index and col_index < len(df.columns):
        val = df.loc[row_name].iloc[col_index]
        return default if pd.isna(val) else float(val)
    return default


def is_valid_row(bs, inc, cfs, i):
    critical_fields = [
        safe_get(bs, "Total Assets", i),
        safe_get(inc, "Total Revenue", i),
        safe_get(inc, "Net Income", i),
    ]
    return any(v is not None for v in critical_fields)


def fetch_ticker(ticker, name, cik):
    try:
        cpy = yf.Ticker(ticker)

        bs    = cpy.balance_sheet
        inc   = cpy.income_stmt
        cfs   = cpy.cashflow
        bs_q  = cpy.quarterly_balance_sheet
        inc_q = cpy.quarterly_income_stmt
        cfs_q = cpy.quarterly_cashflow

        result = {
            "company": Company(ticker=ticker, name=name, cik=cik),
            "balance_sheet_annual":       [],
            "income_statement_annual":    [],
            "cash_flow_annual":           [],
            "balance_sheet_quarterly":    [],
            "income_statement_quarterly": [],
            "cash_flow_quarterly":        [],
        }

        # --- Annual ---
        num_years = len(bs.columns) if bs is not None and not bs.empty else 0
        for i in range(num_years):
            if not is_valid_row(bs, inc, cfs, i):
                logger.debug("Skipping %s annual year %d — no critical data", ticker, i)
                continue
            fiscal_year = bs.columns[i].date()

            result["balance_sheet_annual"].append(BalanceSheetAnnual(
                ticker=ticker,
                fiscal_year=fiscal_year,
                total_assets=safe_get(bs, "Total Assets", i),
                total_liabilities=safe_get(bs, "Total Liabilities Net Minority Interest", i),
                shareholders_equity=safe_get(bs, "Stockholders Equity", i),
                current_assets=safe_get(bs, "Current Assets", i),
                current_liabilities=safe_get(bs, "Current Liabilities", i),
                cash=safe_get(bs, "Cash And Cash Equivalents", i),
                inventory=safe_get(bs, "Inventory", i),
                accounts_receivable=safe_get(bs, "Accounts Receivable", i),
                total_debt=safe_get(bs, "Total Debt", i),
                goodwill=safe_get(bs, "Goodwill", i),
                other_intangible_assets=safe_get(bs, "Other Intangible Assets", i),
            ))

            result["income_statement_annual"].append(IncomeStatementAnnual(
                ticker=ticker,
                fiscal_year=fiscal_year,
                total_revenue=safe_get(inc, "Total Revenue", i),
                cost_of_revenue=safe_get(inc, "Cost Of Revenue", i),
                gross_profit=safe_get(inc, "Gross Profit", i),
                research_and_development=safe_get(inc, "Research And Development", i),
                operating_income=safe_get(inc, "Operating Income", i),
                ebitda=safe_get(inc, "EBITDA", i),
                net_income=safe_get(inc, "Net Income", i),
                interest_expense=safe_get(inc, "Interest Expense", i),
                tax_provision=safe_get(inc, "Tax Provision", i),
                basic_eps=safe_get(inc, "Basic EPS", i),
                diluted_eps=safe_get(inc, "Diluted EPS", i),
            ))

            result["cash_flow_annual"].append(CashFlowAnnual(
                ticker=ticker,
                fiscal_year=fiscal_year,
                operating_cash_flow=safe_get(cfs, "Operating Cash Flow", i),
                capital_expenditure=safe_get(cfs, "Capital Expenditure", i),
                depreciation_amortization=safe_get(cfs, "Depreciation And Amortization", i),
                free_cash_flow=safe_get(cfs, "Free Cash Flow", i),
                stock_based_compensation=safe_get(cfs, "Stock Based Compensation", i),
                change_in_working_capital=safe_get(cfs, "Change In Working Capital", i),
            ))

        # --- Quarterly ---
        num_quarters = len(bs_q.columns) if bs_q is not None and not bs_q.empty else 0
        for i in range(num_quarters):
            if not is_valid_row(bs_q, inc_q, cfs_q, i):
                logger.debug("Skipping %s quarter %d — no critical data", ticker, i)
                continue
            fiscal_quarter = bs_q.columns[i].date()

            result["balance_sheet_quarterly"].append(BalanceSheetQuarterly(
                ticker=ticker,
                fiscal_quarter=fiscal_quarter,
                total_assets=safe_get(bs_q, "Total Assets", i),
                total_liabilities=safe_get(bs_q, "Total Liabilities Net Minority Interest", i),
                shareholders_equity=safe_get(bs_q, "Stockholders Equity", i),
                current_assets=safe_get(bs_q, "Current Assets", i),
                current_liabilities=safe_get(bs_q, "Current Liabilities", i),
                cash=safe_get(bs_q, "Cash And Cash Equivalents", i),
                inventory=safe_get(bs_q, "Inventory", i),
                accounts_receivable=safe_get(bs_q, "Accounts Receivable", i),
                total_debt=safe_get(bs_q, "Total Debt", i),
                goodwill=safe_get(bs_q, "Goodwill", i),
                other_intangible_assets=safe_get(bs_q, "Other Intangible Assets", i),
            ))

            result["income_statement_quarterly"].append(IncomeStatementQuarterly(
                ticker=ticker,
                fiscal_quarter=fiscal_quarter,
                total_revenue=safe_get(inc_q, "Total Revenue", i),
                cost_of_revenue=safe_get(inc_q, "Cost Of Revenue", i),
                gross_profit=safe_get(inc_q, "Gross Profit", i),
                research_and_development=safe_get(inc_q, "Research And Development", i),
                operating_income=safe_get(inc_q, "Operating Income", i),
                ebitda=safe_get(inc_q, "EBITDA", i),
                net_income=safe_get(inc_q, "Net Income", i),
                interest_expense=safe_get(inc_q, "Interest Expense", i),
                tax_provision=safe_get(inc_q, "Tax Provision", i),
                basic_eps=safe_get(inc_q, "Basic EPS", i),
                diluted_eps=safe_get(inc_q, "Diluted EPS", i),
            ))

            result["cash_flow_quarterly"].append(CashFlowQuarterly(
                ticker=ticker,
                fiscal_quarter=fiscal_quarter,
                operating_cash_flow=safe_get(cfs_q, "Operating Cash Flow", i),
                capital_expenditure=safe_get(cfs_q, "Capital Expenditure", i),
                depreciation_amortization=safe_get(cfs_q, "Depreciation And Amortization", i),
                free_cash_flow=safe_get(cfs_q, "Free Cash Flow", i),
                stock_based_compensation=safe_get(cfs_q, "Stock Based Compensation", i),
                change_in_working_capital=safe_get(cfs_q, "Change In Working Capital", i),
            ))

        return result

    except Exception as e:
        logger.error("Error processing %s: %s", ticker, e)
        return None


def fetch_companies_from_sec(limit=100):
    headers = {'User-Agent': "christofferdej.acct@gmail.com"}
    response = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers
    )
    data = pd.DataFrame.from_dict(response.json(), orient='index')
    data['cik_str'] = data['cik_str'].astype(str).str.zfill(10)
    return data[["ticker", "title", "cik_str"]].head(limit)


def run_fetch(limit=100):
    companies = fetch_companies_from_sec(limit)
    results = []

    rows = list(companies.itertuples(index=False))
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        logger.info("Fetching tickers %d–%d of %d", i + 1, i + len(batch), len(rows))
        with ThreadPoolExecutor(max_workers=WORKERS) as executor:
            batch_results = executor.map(
                lambda row: fetch_ticker(row.ticker, row.title, row.cik_str),
                batch
            )
            results.extend(r for r in batch_results if r is not None)
        if i + BATCH_SIZE < len(rows):
            time.sleep(DELAY_BETWEEN_BATCHES)

    return results

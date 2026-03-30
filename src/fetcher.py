import json
import logging
import os
import time
import pandas as pd
import yfinance as yf
import requests
from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log, retry_if_exception_type

from .models import (
    Company,
    BalanceSheetAnnual, IncomeStatementAnnual, CashFlowAnnual,
    BalanceSheetQuarterly, IncomeStatementQuarterly, CashFlowQuarterly,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
DELAY_BETWEEN_BATCHES = 2   # seconds — brief margin between batches
DELAY_BETWEEN_REQUESTS = 1  # seconds — max 1 request/second to Yahoo
INFO_FAILURE_BACKOFF = 5     # seconds — extra backoff when .info fails (likely rate-limited)

# Fields where the yfinance row name doesn't match simple title-casing of the field name.
_YF_NAME_OVERRIDES = {
    # Acronyms
    "net_ppe": "Net PPE",
    "gross_ppe": "Gross PPE",
    "net_ppe_purchase_and_sale": "Net PPE Purchase And Sale",
    "purchase_of_ppe": "Purchase Of PPE",
    "normalized_ebitda": "Normalized EBITDA",
    "ebitda": "EBITDA",
    "ebit": "EBIT",
    "diluted_eps": "Diluted EPS",
    "basic_eps": "Basic EPS",
    "diluted_ni_availto_com_stockholders": "Diluted NI Availto Com Stockholders",
    # yfinance quirks (different names, concatenated words)
    "total_liabilities": "Total Liabilities Net Minority Interest",
    "total_non_current_liabilities": "Total Non Current Liabilities Net Minority Interest",
    "trade_and_other_payables_non_current": "Tradeand Other Payables Non Current",
    "pension_and_other_post_retirement_benefit_plans_current": "Pensionand Other Post Retirement Benefit Plans Current",
    "investment_in_financial_assets": "Investmentin Financial Assets",
    "financial_assets_designated_fvtpl_total": "Financial Assets Designatedas Fair Value Through Profitor Loss Total",
    "cash": "Cash And Cash Equivalents",
    "other_g_and_a": "Other Gand A",
}


class Throttle:
    """Sleeps only the remaining time needed to maintain min_interval between calls."""
    def __init__(self, min_interval: float = 1.0):
        self.min_interval = min_interval
        self.last_call = 0.0

    def wait(self):
        now = time.monotonic()
        elapsed = now - self.last_call
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_call = time.monotonic()


_throttle = Throttle(DELAY_BETWEEN_REQUESTS)


def _yf_row_name(field_name: str) -> str:
    """Convert a model field name to the corresponding yfinance DataFrame row name."""
    return _YF_NAME_OVERRIDES.get(field_name, field_name.replace("_", " ").title())


def _financial_fields(model_class):
    """Return field names that hold financial data (excludes ticker and date fields)."""
    skip = {"ticker", "fiscal_year", "fiscal_quarter"}
    return [name for name in model_class.model_fields if name not in skip]


def _col_index_map(df):
    """Map each column's date to its positional index."""
    if df is None or df.empty:
        return {}
    return {col.date(): idx for idx, col in enumerate(df.columns)}


def safe_get(df, row_name, col_index, default=None):
    if df is None or df.empty or col_index is None:
        return default
    if row_name in df.index and col_index < len(df.columns):
        val = df.loc[row_name].iloc[col_index]
        return default if pd.isna(val) else float(val)
    return default


def is_valid_row(bs, inc, bs_i, inc_i):
    total_assets = safe_get(bs, "Total Assets", bs_i)
    net_income = safe_get(inc, "Net Income", inc_i)
    return total_assets is not None and net_income is not None


def _build_row(df, col_index, ticker, model_class, date_field, date_value):
    """Build a single Pydantic model instance from a yfinance DataFrame column."""
    kwargs = {"ticker": ticker, date_field: date_value}
    for field in _financial_fields(model_class):
        kwargs[field] = safe_get(df, _yf_row_name(field), col_index)
    return model_class(**kwargs)


def _parse_statements(ticker, bs, inc, cfs, bs_model, inc_model, cf_model, date_field):
    """Parse aligned financial statements into lists of model instances.

    Iterates over balance sheet dates, aligns income statement and cash flow
    by date, validates critical fields, and builds model rows.
    """
    bs_dates = _col_index_map(bs)
    inc_dates = _col_index_map(inc)
    cfs_dates = _col_index_map(cfs)

    bs_rows, inc_rows, cf_rows = [], [], []

    for dt, bs_i in bs_dates.items():
        inc_i = inc_dates.get(dt)
        cfs_i = cfs_dates.get(dt)

        if not is_valid_row(bs, inc, bs_i, inc_i):
            logger.debug("Skipping %s %s %s — no critical data", ticker, date_field, dt)
            continue

        bs_rows.append(_build_row(bs, bs_i, ticker, bs_model, date_field, dt))
        inc_rows.append(_build_row(inc, inc_i, ticker, inc_model, date_field, dt))
        cf_rows.append(_build_row(cfs, cfs_i, ticker, cf_model, date_field, dt))

    return bs_rows, inc_rows, cf_rows


@retry(
    retry=retry_if_exception_type((
        requests.exceptions.RequestException,
        ConnectionError,
        IOError,
        json.JSONDecodeError,
    )),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def fetch_ticker(ticker, name, cik):
    cpy = yf.Ticker(ticker)

    try:
        _throttle.wait()
        info = cpy.info or {}
    except Exception as e:
        logger.warning("Failed to fetch info for %s (%s: %s), backing off before continuing",
                       ticker, type(e).__name__, e)
        time.sleep(INFO_FAILURE_BACKOFF)
        info = {}

    _throttle.wait(); bs    = cpy.balance_sheet
    _throttle.wait(); inc   = cpy.income_stmt
    _throttle.wait(); cfs   = cpy.cashflow
    _throttle.wait(); bs_q  = cpy.quarterly_balance_sheet
    _throttle.wait(); inc_q = cpy.quarterly_income_stmt
    _throttle.wait(); cfs_q = cpy.quarterly_cashflow

    result = {
        "company": Company(
            ticker=ticker,
            name=name,
            cik=cik,
            sector=info.get("sector"),
            industry=info.get("industry"),
            country=info.get("country"),
            city=info.get("city"),
            state=info.get("state"),
            website=info.get("website"),
            description=info.get("longBusinessSummary"),
            full_time_employees=info.get("fullTimeEmployees"),
            exchange=info.get("exchange"),
            currency=info.get("financialCurrency"),
            quote_type=info.get("quoteType"),
        ),
    }

    bs_a, inc_a, cf_a = _parse_statements(
        ticker, bs, inc, cfs,
        BalanceSheetAnnual, IncomeStatementAnnual, CashFlowAnnual,
        "fiscal_year",
    )
    result["balance_sheet_annual"] = bs_a
    result["income_statement_annual"] = inc_a
    result["cash_flow_annual"] = cf_a

    bs_q_rows, inc_q_rows, cf_q_rows = _parse_statements(
        ticker, bs_q, inc_q, cfs_q,
        BalanceSheetQuarterly, IncomeStatementQuarterly, CashFlowQuarterly,
        "fiscal_quarter",
    )
    result["balance_sheet_quarterly"] = bs_q_rows
    result["income_statement_quarterly"] = inc_q_rows
    result["cash_flow_quarterly"] = cf_q_rows

    if not result["balance_sheet_annual"]:
        logger.warning("Skipping %s — no valid annual rows (missing Total Assets or Net Income)", ticker)
        return None

    annual_years = len(result["balance_sheet_annual"])
    quarterly_periods = len(result["balance_sheet_quarterly"])
    logger.info("Fetched %s — %d annual years, %d quarterly periods", ticker, annual_years, quarterly_periods)
    return result


def fetch_companies_from_sec(limit=25, exclude: set = None):
    user_agent = os.getenv("SEC_USER_AGENT")
    if not user_agent:
        raise RuntimeError("SEC_USER_AGENT must be set in .env")
    headers = {'User-Agent': user_agent}
    response = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    data = pd.DataFrame.from_dict(response.json(), orient='index')
    data['cik_str'] = data['cik_str'].astype(str).str.zfill(10)
    if exclude:
        data = data[~data["ticker"].isin(exclude)]
    return data[["ticker", "title", "cik_str"]].head(limit)


def run_fetch(limit=25, exclude: set = None):
    companies = fetch_companies_from_sec(limit, exclude=exclude)
    results = []
    skipped = 0
    failed = 0
    skipped_tickers = []
    failed_tickers = []

    rows = list(companies.itertuples(index=False))
    logger.info("Fetching %d tickers from SEC (excluded %d already processed)",
                len(rows), limit - len(rows) if len(rows) < limit else 0)
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        logger.info("Fetching batch %d–%d of %d", i + 1, i + len(batch), len(rows))
        for row in batch:
            t0 = time.monotonic()
            try:
                r = fetch_ticker(row.ticker, row.title, row.cik_str)
                elapsed = time.monotonic() - t0
                if r is not None:
                    results.append(r)
                else:
                    skipped += 1
                    skipped_tickers.append(row.ticker)
            except Exception as e:
                elapsed = time.monotonic() - t0
                failed += 1
                failed_tickers.append(row.ticker)
                logger.error("Failed to fetch %s after %.1fs (%s): %s", row.ticker, elapsed, type(e).__name__, e)
        if i + BATCH_SIZE < len(rows):
            time.sleep(DELAY_BETWEEN_BATCHES)

    logger.info("Fetch complete: %d succeeded, %d skipped (no data), %d failed",
                len(results), skipped, failed)
    if skipped_tickers:
        logger.info("Skipped tickers: %s", ", ".join(skipped_tickers))
    if failed_tickers:
        logger.warning("Failed tickers: %s", ", ".join(failed_tickers))
    return results

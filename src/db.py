import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

logger = logging.getLogger(__name__)

FINANCIAL_TABLES = [
    "balance_sheet_annual",
    "income_statement_annual",
    "cash_flow_annual",
    "balance_sheet_quarterly",
    "income_statement_quarterly",
    "cash_flow_quarterly",
]


def get_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _serialize_financial_rows(models: list) -> list[dict]:
    """Convert Pydantic models to dicts, stripping the ticker field."""
    rows = []
    for m in models:
        d = m.model_dump(mode="json")
        d.pop("ticker")
        rows.append(d)
    return rows


def _build_rpc_payload(result: dict) -> dict:
    """Build the JSONB payload expected by the upsert_company_financials RPC."""
    company = result["company"]
    payload = {
        "ticker": company.ticker,
        "name": company.name,
        "cik": company.cik,
    }
    for table in FINANCIAL_TABLES:
        payload[table] = _serialize_financial_rows(result.get(table, []))
    return payload


def get_processed_tickers(client: Client) -> set[str]:
    try:
        response = client.table("companies").select("ticker").execute()
        return {row["ticker"] for row in response.data}
    except Exception as e:
        logger.error("Failed to fetch processed tickers from DB: %s", e)
        raise


def upsert_result(client: Client, result: dict) -> int:
    """Upsert a single company + all its financials via a database transaction (RPC)."""
    ticker = result["company"].ticker
    payload = _build_rpc_payload(result)
    row_counts = {t: len(payload[t]) for t in FINANCIAL_TABLES if payload[t]}
    logger.debug("RPC payload for %s: %s", ticker, row_counts)
    response = client.rpc("upsert_company_financials", {"payload": payload}).execute()
    logger.debug("RPC returned company_id=%s for %s", response.data, ticker)
    return response.data

import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

logger = logging.getLogger(__name__)


def get_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _serialize_financial(models: list, company_id: int) -> list[dict]:
    rows = []
    for m in models:
        d = m.model_dump(mode="json")
        d.pop("ticker")
        d["company_id"] = company_id
        rows.append(d)
    return rows


def upsert_company(client: Client, company) -> int:
    data = company.model_dump(mode="json", exclude={"id"})
    response = client.table("companies").upsert(data, on_conflict="ticker").execute()
    return response.data[0]["id"]


def upsert_balance_sheet_annual(client: Client, rows: list, company_id: int):
    client.table("balance_sheet_annual").upsert(_serialize_financial(rows, company_id)).execute()


def upsert_income_statement_annual(client: Client, rows: list, company_id: int):
    client.table("income_statement_annual").upsert(_serialize_financial(rows, company_id)).execute()


def upsert_cash_flow_annual(client: Client, rows: list, company_id: int):
    client.table("cash_flow_annual").upsert(_serialize_financial(rows, company_id)).execute()


def upsert_balance_sheet_quarterly(client: Client, rows: list, company_id: int):
    client.table("balance_sheet_quarterly").upsert(_serialize_financial(rows, company_id)).execute()


def upsert_income_statement_quarterly(client: Client, rows: list, company_id: int):
    client.table("income_statement_quarterly").upsert(_serialize_financial(rows, company_id)).execute()


def upsert_cash_flow_quarterly(client: Client, rows: list, company_id: int):
    client.table("cash_flow_quarterly").upsert(_serialize_financial(rows, company_id)).execute()


def get_processed_tickers(client: Client) -> set[str]:
    try:
        response = client.table("companies").select("ticker").execute()
        return {row["ticker"] for row in response.data}
    except Exception as e:
        logger.error("Failed to fetch processed tickers from DB: %s", e)
        raise


def upsert_result(client: Client, result: dict):
    company_id = upsert_company(client, result["company"])
    try:
        upsert_balance_sheet_annual(client, result["balance_sheet_annual"], company_id)
        upsert_income_statement_annual(client, result["income_statement_annual"], company_id)
        upsert_cash_flow_annual(client, result["cash_flow_annual"], company_id)
        upsert_balance_sheet_quarterly(client, result["balance_sheet_quarterly"], company_id)
        upsert_income_statement_quarterly(client, result["income_statement_quarterly"], company_id)
        upsert_cash_flow_quarterly(client, result["cash_flow_quarterly"], company_id)
    except Exception:
        client.table("companies").delete().eq("id", company_id).execute()
        raise

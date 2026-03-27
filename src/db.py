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


def _check_response(response, table_name: str):
    if not response.data and response.data != []:
        raise RuntimeError(f"Supabase upsert to '{table_name}' returned no data")


def upsert_company(client: Client, company) -> int:
    data = company.model_dump(mode="json", exclude={"id"})
    response = client.table("companies").upsert(data, on_conflict="ticker").execute()
    _check_response(response, "companies")
    return response.data[0]["id"]


def _upsert_financial(client: Client, table: str, rows: list, company_id: int):
    if not rows:
        return
    response = client.table(table).upsert(_serialize_financial(rows, company_id)).execute()
    _check_response(response, table)


def get_processed_tickers(client: Client) -> set[str]:
    try:
        response = client.table("companies").select("ticker").execute()
        return {row["ticker"] for row in response.data}
    except Exception as e:
        logger.error("Failed to fetch processed tickers from DB: %s", e)
        raise


FINANCIAL_TABLES = [
    "balance_sheet_annual",
    "income_statement_annual",
    "cash_flow_annual",
    "balance_sheet_quarterly",
    "income_statement_quarterly",
    "cash_flow_quarterly",
]


def upsert_result(client: Client, result: dict):
    company_id = upsert_company(client, result["company"])
    inserted_tables = []
    try:
        for table in FINANCIAL_TABLES:
            _upsert_financial(client, table, result[table], company_id)
            inserted_tables.append(table)
    except Exception:
        for table in inserted_tables:
            try:
                client.table(table).delete().eq("company_id", company_id).execute()
            except Exception as cleanup_err:
                logger.error("Rollback failed for %s (company_id=%d): %s", table, company_id, cleanup_err)
        try:
            client.table("companies").delete().eq("id", company_id).execute()
        except Exception as cleanup_err:
            logger.error("Rollback failed for companies (id=%d): %s", company_id, cleanup_err)
        raise

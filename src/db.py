import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

logger = logging.getLogger(__name__)


def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _serialize(models: list) -> list[dict]:
    return [m.model_dump(mode="json") for m in models]


def upsert_company(client: Client, company):
    client.table("companies").upsert(company.model_dump(mode="json")).execute()


def upsert_balance_sheet_annual(client: Client, rows: list):
    client.table("balance_sheet_annual").upsert(_serialize(rows)).execute()


def upsert_income_statement_annual(client: Client, rows: list):
    client.table("income_statement_annual").upsert(_serialize(rows)).execute()


def upsert_cash_flow_annual(client: Client, rows: list):
    client.table("cash_flow_annual").upsert(_serialize(rows)).execute()


def upsert_balance_sheet_quarterly(client: Client, rows: list):
    client.table("balance_sheet_quarterly").upsert(_serialize(rows)).execute()


def upsert_income_statement_quarterly(client: Client, rows: list):
    client.table("income_statement_quarterly").upsert(_serialize(rows)).execute()


def upsert_cash_flow_quarterly(client: Client, rows: list):
    client.table("cash_flow_quarterly").upsert(_serialize(rows)).execute()


def insert_result(client: Client, result: dict):
    upsert_company(client, result["company"])
    upsert_balance_sheet_annual(client, result["balance_sheet_annual"])
    upsert_income_statement_annual(client, result["income_statement_annual"])
    upsert_cash_flow_annual(client, result["cash_flow_annual"])
    upsert_balance_sheet_quarterly(client, result["balance_sheet_quarterly"])
    upsert_income_statement_quarterly(client, result["income_statement_quarterly"])
    upsert_cash_flow_quarterly(client, result["cash_flow_quarterly"])

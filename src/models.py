from pydantic import BaseModel
from datetime import date


class Company(BaseModel):
    ticker: str
    name: str | None
    cik: str | None


class BalanceSheetAnnual(BaseModel):
    ticker: str
    fiscal_year: date
    total_assets: float | None
    total_liabilities: float | None
    shareholders_equity: float | None
    current_assets: float | None
    current_liabilities: float | None
    cash: float | None
    inventory: float | None
    accounts_receivable: float | None
    total_debt: float | None
    goodwill: float | None
    other_intangible_assets: float | None


class IncomeStatementAnnual(BaseModel):
    ticker: str
    fiscal_year: date
    total_revenue: float | None
    cost_of_revenue: float | None
    gross_profit: float | None
    research_and_development: float | None
    operating_income: float | None
    ebitda: float | None
    net_income: float | None
    interest_expense: float | None
    tax_provision: float | None
    basic_eps: float | None
    diluted_eps: float | None


class CashFlowAnnual(BaseModel):
    ticker: str
    fiscal_year: date
    operating_cash_flow: float | None
    capital_expenditure: float | None
    depreciation_amortization: float | None
    free_cash_flow: float | None
    stock_based_compensation: float | None
    change_in_working_capital: float | None


class BalanceSheetQuarterly(BaseModel):
    ticker: str
    fiscal_quarter: date
    total_assets: float | None
    total_liabilities: float | None
    shareholders_equity: float | None
    current_assets: float | None
    current_liabilities: float | None
    cash: float | None
    inventory: float | None
    accounts_receivable: float | None
    total_debt: float | None
    goodwill: float | None
    other_intangible_assets: float | None


class IncomeStatementQuarterly(BaseModel):
    ticker: str
    fiscal_quarter: date
    total_revenue: float | None
    cost_of_revenue: float | None
    gross_profit: float | None
    research_and_development: float | None
    operating_income: float | None
    ebitda: float | None
    net_income: float | None
    interest_expense: float | None
    tax_provision: float | None
    basic_eps: float | None
    diluted_eps: float | None


class CashFlowQuarterly(BaseModel):
    ticker: str
    fiscal_quarter: date
    operating_cash_flow: float | None
    capital_expenditure: float | None
    depreciation_amortization: float | None
    free_cash_flow: float | None
    stock_based_compensation: float | None
    change_in_working_capital: float | None

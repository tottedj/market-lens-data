import logging
import os
import time
import pandas as pd
import yfinance as yf
import numpy as np
import requests
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log, retry_if_exception_type

from models import (
    Company,
    BalanceSheetAnnual, IncomeStatementAnnual, CashFlowAnnual,
    BalanceSheetQuarterly, IncomeStatementQuarterly, CashFlowQuarterly,
)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
DELAY_BETWEEN_BATCHES = 2   # seconds — brief margin between batches
DELAY_BETWEEN_REQUESTS = 1  # seconds — max 1 request/second to Yahoo


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


def safe_get(df, row_name, col_index, default=None):
    if df is None or df.empty:
        return default
    if row_name in df.index and col_index < len(df.columns):
        val = df.loc[row_name].iloc[col_index]
        return default if pd.isna(val) else float(val)
    return default


def is_valid_row(bs, inc, i):
    total_assets = safe_get(bs, "Total Assets", i)
    net_income = safe_get(inc, "Net Income", i)
    return total_assets is not None and net_income is not None


@retry(
    retry=retry_if_exception_type((requests.exceptions.RequestException, ConnectionError, IOError)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def fetch_ticker(ticker, name, cik):
    cpy = yf.Ticker(ticker)

    try:
        _throttle.wait(); info = cpy.info or {}
    except Exception:
        logger.warning("Failed to fetch info for %s, continuing without it", ticker)
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
        if not is_valid_row(bs, inc, i):
            logger.debug("Skipping %s annual year %d — no critical data", ticker, i)
            continue
        fiscal_year = bs.columns[i].date()

        result["balance_sheet_annual"].append(BalanceSheetAnnual(
            ticker=ticker,
            fiscal_year=fiscal_year,
            ordinary_shares_number=safe_get(bs, "Ordinary Shares Number", i),
            share_issued=safe_get(bs, "Share Issued", i),
            common_stock_equity=safe_get(bs, "Common Stock Equity", i),
            stockholders_equity=safe_get(bs, "Stockholders Equity", i),
            total_equity_gross_minority_interest=safe_get(bs, "Total Equity Gross Minority Interest", i),
            retained_earnings=safe_get(bs, "Retained Earnings", i),
            capital_stock=safe_get(bs, "Capital Stock", i),
            common_stock=safe_get(bs, "Common Stock", i),
            gains_losses_not_affecting_retained_earnings=safe_get(bs, "Gains Losses Not Affecting Retained Earnings", i),
            other_equity_adjustments=safe_get(bs, "Other Equity Adjustments", i),
            net_debt=safe_get(bs, "Net Debt", i),
            total_debt=safe_get(bs, "Total Debt", i),
            invested_capital=safe_get(bs, "Invested Capital", i),
            working_capital=safe_get(bs, "Working Capital", i),
            net_tangible_assets=safe_get(bs, "Net Tangible Assets", i),
            tangible_book_value=safe_get(bs, "Tangible Book Value", i),
            capital_lease_obligations=safe_get(bs, "Capital Lease Obligations", i),
            total_capitalization=safe_get(bs, "Total Capitalization", i),
            total_liabilities=safe_get(bs, "Total Liabilities Net Minority Interest", i),
            total_non_current_liabilities=safe_get(bs, "Total Non Current Liabilities Net Minority Interest", i),
            other_non_current_liabilities=safe_get(bs, "Other Non Current Liabilities", i),
            trade_and_other_payables_non_current=safe_get(bs, "Tradeand Other Payables Non Current", i),
            non_current_deferred_liabilities=safe_get(bs, "Non Current Deferred Liabilities", i),
            non_current_deferred_revenue=safe_get(bs, "Non Current Deferred Revenue", i),
            non_current_deferred_taxes_liabilities=safe_get(bs, "Non Current Deferred Taxes Liabilities", i),
            long_term_debt_and_capital_lease_obligation=safe_get(bs, "Long Term Debt And Capital Lease Obligation", i),
            long_term_capital_lease_obligation=safe_get(bs, "Long Term Capital Lease Obligation", i),
            long_term_debt=safe_get(bs, "Long Term Debt", i),
            current_liabilities=safe_get(bs, "Current Liabilities", i),
            other_current_liabilities=safe_get(bs, "Other Current Liabilities", i),
            current_deferred_liabilities=safe_get(bs, "Current Deferred Liabilities", i),
            current_deferred_revenue=safe_get(bs, "Current Deferred Revenue", i),
            current_debt_and_capital_lease_obligation=safe_get(bs, "Current Debt And Capital Lease Obligation", i),
            current_debt=safe_get(bs, "Current Debt", i),
            other_current_borrowings=safe_get(bs, "Other Current Borrowings", i),
            commercial_paper=safe_get(bs, "Commercial Paper", i),
            pension_and_other_post_retirement_benefit_plans_current=safe_get(bs, "Pensionand Other Post Retirement Benefit Plans Current", i),
            payables_and_accrued_expenses=safe_get(bs, "Payables And Accrued Expenses", i),
            payables=safe_get(bs, "Payables", i),
            total_tax_payable=safe_get(bs, "Total Tax Payable", i),
            income_tax_payable=safe_get(bs, "Income Tax Payable", i),
            accounts_payable=safe_get(bs, "Accounts Payable", i),
            total_assets=safe_get(bs, "Total Assets", i),
            total_non_current_assets=safe_get(bs, "Total Non Current Assets", i),
            other_non_current_assets=safe_get(bs, "Other Non Current Assets", i),
            financial_assets=safe_get(bs, "Financial Assets", i),
            investments_and_advances=safe_get(bs, "Investments And Advances", i),
            investment_in_financial_assets=safe_get(bs, "Investmentin Financial Assets", i),
            available_for_sale_securities=safe_get(bs, "Available For Sale Securities", i),
            financial_assets_designated_fvtpl_total=safe_get(bs, "Financial Assets Designatedas Fair Value Through Profitor Loss Total", i),
            long_term_equity_investment=safe_get(bs, "Long Term Equity Investment", i),
            goodwill_and_other_intangible_assets=safe_get(bs, "Goodwill And Other Intangible Assets", i),
            other_intangible_assets=safe_get(bs, "Other Intangible Assets", i),
            goodwill=safe_get(bs, "Goodwill", i),
            net_ppe=safe_get(bs, "Net PPE", i),
            accumulated_depreciation=safe_get(bs, "Accumulated Depreciation", i),
            gross_ppe=safe_get(bs, "Gross PPE", i),
            leases=safe_get(bs, "Leases", i),
            other_properties=safe_get(bs, "Other Properties", i),
            machinery_furniture_equipment=safe_get(bs, "Machinery Furniture Equipment", i),
            buildings_and_improvements=safe_get(bs, "Buildings And Improvements", i),
            land_and_improvements=safe_get(bs, "Land And Improvements", i),
            properties=safe_get(bs, "Properties", i),
            current_assets=safe_get(bs, "Current Assets", i),
            other_current_assets=safe_get(bs, "Other Current Assets", i),
            hedging_assets_current=safe_get(bs, "Hedging Assets Current", i),
            inventory=safe_get(bs, "Inventory", i),
            finished_goods=safe_get(bs, "Finished Goods", i),
            work_in_process=safe_get(bs, "Work In Process", i),
            raw_materials=safe_get(bs, "Raw Materials", i),
            receivables=safe_get(bs, "Receivables", i),
            accounts_receivable=safe_get(bs, "Accounts Receivable", i),
            allowance_for_doubtful_accounts_receivable=safe_get(bs, "Allowance For Doubtful Accounts Receivable", i),
            gross_accounts_receivable=safe_get(bs, "Gross Accounts Receivable", i),
            cash_cash_equivalents_and_short_term_investments=safe_get(bs, "Cash Cash Equivalents And Short Term Investments", i),
            other_short_term_investments=safe_get(bs, "Other Short Term Investments", i),
            cash=safe_get(bs, "Cash And Cash Equivalents", i),
            cash_equivalents=safe_get(bs, "Cash Equivalents", i),
            cash_financial=safe_get(bs, "Cash Financial", i),
        ))

        result["income_statement_annual"].append(IncomeStatementAnnual(
            ticker=ticker,
            fiscal_year=fiscal_year,
            tax_effect_of_unusual_items=safe_get(inc, "Tax Effect Of Unusual Items", i),
            tax_rate_for_calcs=safe_get(inc, "Tax Rate For Calcs", i),
            normalized_ebitda=safe_get(inc, "Normalized EBITDA", i),
            total_unusual_items=safe_get(inc, "Total Unusual Items", i),
            total_unusual_items_excluding_goodwill=safe_get(inc, "Total Unusual Items Excluding Goodwill", i),
            net_income_from_continuing_operation_net_minority_interest=safe_get(inc, "Net Income From Continuing Operation Net Minority Interest", i),
            reconciled_depreciation=safe_get(inc, "Reconciled Depreciation", i),
            reconciled_cost_of_revenue=safe_get(inc, "Reconciled Cost Of Revenue", i),
            ebitda=safe_get(inc, "EBITDA", i),
            ebit=safe_get(inc, "EBIT", i),
            net_interest_income=safe_get(inc, "Net Interest Income", i),
            interest_expense=safe_get(inc, "Interest Expense", i),
            interest_income=safe_get(inc, "Interest Income", i),
            normalized_income=safe_get(inc, "Normalized Income", i),
            net_income_from_continuing_and_discontinued_operation=safe_get(inc, "Net Income From Continuing And Discontinued Operation", i),
            total_expenses=safe_get(inc, "Total Expenses", i),
            total_operating_income_as_reported=safe_get(inc, "Total Operating Income As Reported", i),
            diluted_average_shares=safe_get(inc, "Diluted Average Shares", i),
            basic_average_shares=safe_get(inc, "Basic Average Shares", i),
            diluted_eps=safe_get(inc, "Diluted EPS", i),
            basic_eps=safe_get(inc, "Basic EPS", i),
            diluted_ni_availto_com_stockholders=safe_get(inc, "Diluted NI Availto Com Stockholders", i),
            net_income_common_stockholders=safe_get(inc, "Net Income Common Stockholders", i),
            net_income=safe_get(inc, "Net Income", i),
            net_income_including_noncontrolling_interests=safe_get(inc, "Net Income Including Noncontrolling Interests", i),
            net_income_continuous_operations=safe_get(inc, "Net Income Continuous Operations", i),
            tax_provision=safe_get(inc, "Tax Provision", i),
            pretax_income=safe_get(inc, "Pretax Income", i),
            other_income_expense=safe_get(inc, "Other Income Expense", i),
            other_non_operating_income_expenses=safe_get(inc, "Other Non Operating Income Expenses", i),
            special_income_charges=safe_get(inc, "Special Income Charges", i),
            write_off=safe_get(inc, "Write Off", i),
            gain_on_sale_of_security=safe_get(inc, "Gain On Sale Of Security", i),
            net_non_operating_interest_income_expense=safe_get(inc, "Net Non Operating Interest Income Expense", i),
            interest_expense_non_operating=safe_get(inc, "Interest Expense Non Operating", i),
            interest_income_non_operating=safe_get(inc, "Interest Income Non Operating", i),
            operating_income=safe_get(inc, "Operating Income", i),
            operating_expense=safe_get(inc, "Operating Expense", i),
            research_and_development=safe_get(inc, "Research And Development", i),
            selling_general_and_administration=safe_get(inc, "Selling General And Administration", i),
            selling_and_marketing_expense=safe_get(inc, "Selling And Marketing Expense", i),
            general_and_administrative_expense=safe_get(inc, "General And Administrative Expense", i),
            other_g_and_a=safe_get(inc, "Other Gand A", i),
            gross_profit=safe_get(inc, "Gross Profit", i),
            cost_of_revenue=safe_get(inc, "Cost Of Revenue", i),
            total_revenue=safe_get(inc, "Total Revenue", i),
            operating_revenue=safe_get(inc, "Operating Revenue", i),
        ))

        result["cash_flow_annual"].append(CashFlowAnnual(
            ticker=ticker,
            fiscal_year=fiscal_year,
            free_cash_flow=safe_get(cfs, "Free Cash Flow", i),
            repurchase_of_capital_stock=safe_get(cfs, "Repurchase Of Capital Stock", i),
            repayment_of_debt=safe_get(cfs, "Repayment Of Debt", i),
            issuance_of_debt=safe_get(cfs, "Issuance Of Debt", i),
            issuance_of_capital_stock=safe_get(cfs, "Issuance Of Capital Stock", i),
            capital_expenditure=safe_get(cfs, "Capital Expenditure", i),
            end_cash_position=safe_get(cfs, "End Cash Position", i),
            beginning_cash_position=safe_get(cfs, "Beginning Cash Position", i),
            effect_of_exchange_rate_changes=safe_get(cfs, "Effect Of Exchange Rate Changes", i),
            changes_in_cash=safe_get(cfs, "Changes In Cash", i),
            financing_cash_flow=safe_get(cfs, "Financing Cash Flow", i),
            cash_flow_from_continuing_financing_activities=safe_get(cfs, "Cash Flow From Continuing Financing Activities", i),
            net_other_financing_charges=safe_get(cfs, "Net Other Financing Charges", i),
            cash_dividends_paid=safe_get(cfs, "Cash Dividends Paid", i),
            common_stock_dividend_paid=safe_get(cfs, "Common Stock Dividend Paid", i),
            net_common_stock_issuance=safe_get(cfs, "Net Common Stock Issuance", i),
            common_stock_payments=safe_get(cfs, "Common Stock Payments", i),
            common_stock_issuance=safe_get(cfs, "Common Stock Issuance", i),
            net_issuance_payments_of_debt=safe_get(cfs, "Net Issuance Payments Of Debt", i),
            net_short_term_debt_issuance=safe_get(cfs, "Net Short Term Debt Issuance", i),
            short_term_debt_issuance=safe_get(cfs, "Short Term Debt Issuance", i),
            short_term_debt_payments=safe_get(cfs, "Short Term Debt Payments", i),
            net_long_term_debt_issuance=safe_get(cfs, "Net Long Term Debt Issuance", i),
            long_term_debt_payments=safe_get(cfs, "Long Term Debt Payments", i),
            long_term_debt_issuance=safe_get(cfs, "Long Term Debt Issuance", i),
            investing_cash_flow=safe_get(cfs, "Investing Cash Flow", i),
            cash_flow_from_continuing_investing_activities=safe_get(cfs, "Cash Flow From Continuing Investing Activities", i),
            net_other_investing_changes=safe_get(cfs, "Net Other Investing Changes", i),
            net_investment_purchase_and_sale=safe_get(cfs, "Net Investment Purchase And Sale", i),
            sale_of_investment=safe_get(cfs, "Sale Of Investment", i),
            purchase_of_investment=safe_get(cfs, "Purchase Of Investment", i),
            net_business_purchase_and_sale=safe_get(cfs, "Net Business Purchase And Sale", i),
            purchase_of_business=safe_get(cfs, "Purchase Of Business", i),
            net_ppe_purchase_and_sale=safe_get(cfs, "Net PPE Purchase And Sale", i),
            purchase_of_ppe=safe_get(cfs, "Purchase Of PPE", i),
            operating_cash_flow=safe_get(cfs, "Operating Cash Flow", i),
            cash_flow_from_continuing_operating_activities=safe_get(cfs, "Cash Flow From Continuing Operating Activities", i),
            change_in_working_capital=safe_get(cfs, "Change In Working Capital", i),
            change_in_other_working_capital=safe_get(cfs, "Change In Other Working Capital", i),
            change_in_other_current_liabilities=safe_get(cfs, "Change In Other Current Liabilities", i),
            change_in_other_current_assets=safe_get(cfs, "Change In Other Current Assets", i),
            change_in_payables_and_accrued_expense=safe_get(cfs, "Change In Payables And Accrued Expense", i),
            change_in_payable=safe_get(cfs, "Change In Payable", i),
            change_in_account_payable=safe_get(cfs, "Change In Account Payable", i),
            change_in_tax_payable=safe_get(cfs, "Change In Tax Payable", i),
            change_in_income_tax_payable=safe_get(cfs, "Change In Income Tax Payable", i),
            change_in_inventory=safe_get(cfs, "Change In Inventory", i),
            change_in_receivables=safe_get(cfs, "Change In Receivables", i),
            changes_in_account_receivables=safe_get(cfs, "Changes In Account Receivables", i),
            stock_based_compensation=safe_get(cfs, "Stock Based Compensation", i),
            unrealized_gain_loss_on_investment_securities=safe_get(cfs, "Unrealized Gain Loss On Investment Securities", i),
            asset_impairment_charge=safe_get(cfs, "Asset Impairment Charge", i),
            deferred_tax=safe_get(cfs, "Deferred Tax", i),
            deferred_income_tax=safe_get(cfs, "Deferred Income Tax", i),
            depreciation_amortization_depletion=safe_get(cfs, "Depreciation Amortization Depletion", i),
            depreciation_and_amortization=safe_get(cfs, "Depreciation And Amortization", i),
            depreciation=safe_get(cfs, "Depreciation", i),
            operating_gains_losses=safe_get(cfs, "Operating Gains Losses", i),
            gain_loss_on_investment_securities=safe_get(cfs, "Gain Loss On Investment Securities", i),
            net_income_from_continuing_operations=safe_get(cfs, "Net Income From Continuing Operations", i),
        ))

    # --- Quarterly ---
    num_quarters = len(bs_q.columns) if bs_q is not None and not bs_q.empty else 0
    for i in range(num_quarters):
        if not is_valid_row(bs_q, inc_q, i):
            logger.debug("Skipping %s quarter %d — no critical data", ticker, i)
            continue
        fiscal_quarter = bs_q.columns[i].date()

        result["balance_sheet_quarterly"].append(BalanceSheetQuarterly(
            ticker=ticker,
            fiscal_quarter=fiscal_quarter,
            ordinary_shares_number=safe_get(bs_q, "Ordinary Shares Number", i),
            share_issued=safe_get(bs_q, "Share Issued", i),
            common_stock_equity=safe_get(bs_q, "Common Stock Equity", i),
            stockholders_equity=safe_get(bs_q, "Stockholders Equity", i),
            total_equity_gross_minority_interest=safe_get(bs_q, "Total Equity Gross Minority Interest", i),
            retained_earnings=safe_get(bs_q, "Retained Earnings", i),
            capital_stock=safe_get(bs_q, "Capital Stock", i),
            common_stock=safe_get(bs_q, "Common Stock", i),
            gains_losses_not_affecting_retained_earnings=safe_get(bs_q, "Gains Losses Not Affecting Retained Earnings", i),
            other_equity_adjustments=safe_get(bs_q, "Other Equity Adjustments", i),
            net_debt=safe_get(bs_q, "Net Debt", i),
            total_debt=safe_get(bs_q, "Total Debt", i),
            invested_capital=safe_get(bs_q, "Invested Capital", i),
            working_capital=safe_get(bs_q, "Working Capital", i),
            net_tangible_assets=safe_get(bs_q, "Net Tangible Assets", i),
            tangible_book_value=safe_get(bs_q, "Tangible Book Value", i),
            capital_lease_obligations=safe_get(bs_q, "Capital Lease Obligations", i),
            total_capitalization=safe_get(bs_q, "Total Capitalization", i),
            total_liabilities=safe_get(bs_q, "Total Liabilities Net Minority Interest", i),
            total_non_current_liabilities=safe_get(bs_q, "Total Non Current Liabilities Net Minority Interest", i),
            other_non_current_liabilities=safe_get(bs_q, "Other Non Current Liabilities", i),
            trade_and_other_payables_non_current=safe_get(bs_q, "Tradeand Other Payables Non Current", i),
            non_current_deferred_liabilities=safe_get(bs_q, "Non Current Deferred Liabilities", i),
            non_current_deferred_revenue=safe_get(bs_q, "Non Current Deferred Revenue", i),
            non_current_deferred_taxes_liabilities=safe_get(bs_q, "Non Current Deferred Taxes Liabilities", i),
            long_term_debt_and_capital_lease_obligation=safe_get(bs_q, "Long Term Debt And Capital Lease Obligation", i),
            long_term_capital_lease_obligation=safe_get(bs_q, "Long Term Capital Lease Obligation", i),
            long_term_debt=safe_get(bs_q, "Long Term Debt", i),
            current_liabilities=safe_get(bs_q, "Current Liabilities", i),
            other_current_liabilities=safe_get(bs_q, "Other Current Liabilities", i),
            current_deferred_liabilities=safe_get(bs_q, "Current Deferred Liabilities", i),
            current_deferred_revenue=safe_get(bs_q, "Current Deferred Revenue", i),
            current_debt_and_capital_lease_obligation=safe_get(bs_q, "Current Debt And Capital Lease Obligation", i),
            current_debt=safe_get(bs_q, "Current Debt", i),
            other_current_borrowings=safe_get(bs_q, "Other Current Borrowings", i),
            commercial_paper=safe_get(bs_q, "Commercial Paper", i),
            pension_and_other_post_retirement_benefit_plans_current=safe_get(bs_q, "Pensionand Other Post Retirement Benefit Plans Current", i),
            payables_and_accrued_expenses=safe_get(bs_q, "Payables And Accrued Expenses", i),
            payables=safe_get(bs_q, "Payables", i),
            total_tax_payable=safe_get(bs_q, "Total Tax Payable", i),
            income_tax_payable=safe_get(bs_q, "Income Tax Payable", i),
            accounts_payable=safe_get(bs_q, "Accounts Payable", i),
            total_assets=safe_get(bs_q, "Total Assets", i),
            total_non_current_assets=safe_get(bs_q, "Total Non Current Assets", i),
            other_non_current_assets=safe_get(bs_q, "Other Non Current Assets", i),
            financial_assets=safe_get(bs_q, "Financial Assets", i),
            investments_and_advances=safe_get(bs_q, "Investments And Advances", i),
            investment_in_financial_assets=safe_get(bs_q, "Investmentin Financial Assets", i),
            available_for_sale_securities=safe_get(bs_q, "Available For Sale Securities", i),
            financial_assets_designated_fvtpl_total=safe_get(bs_q, "Financial Assets Designatedas Fair Value Through Profitor Loss Total", i),
            long_term_equity_investment=safe_get(bs_q, "Long Term Equity Investment", i),
            goodwill_and_other_intangible_assets=safe_get(bs_q, "Goodwill And Other Intangible Assets", i),
            other_intangible_assets=safe_get(bs_q, "Other Intangible Assets", i),
            goodwill=safe_get(bs_q, "Goodwill", i),
            net_ppe=safe_get(bs_q, "Net PPE", i),
            accumulated_depreciation=safe_get(bs_q, "Accumulated Depreciation", i),
            gross_ppe=safe_get(bs_q, "Gross PPE", i),
            leases=safe_get(bs_q, "Leases", i),
            other_properties=safe_get(bs_q, "Other Properties", i),
            machinery_furniture_equipment=safe_get(bs_q, "Machinery Furniture Equipment", i),
            buildings_and_improvements=safe_get(bs_q, "Buildings And Improvements", i),
            land_and_improvements=safe_get(bs_q, "Land And Improvements", i),
            properties=safe_get(bs_q, "Properties", i),
            current_assets=safe_get(bs_q, "Current Assets", i),
            other_current_assets=safe_get(bs_q, "Other Current Assets", i),
            hedging_assets_current=safe_get(bs_q, "Hedging Assets Current", i),
            inventory=safe_get(bs_q, "Inventory", i),
            finished_goods=safe_get(bs_q, "Finished Goods", i),
            work_in_process=safe_get(bs_q, "Work In Process", i),
            raw_materials=safe_get(bs_q, "Raw Materials", i),
            receivables=safe_get(bs_q, "Receivables", i),
            accounts_receivable=safe_get(bs_q, "Accounts Receivable", i),
            allowance_for_doubtful_accounts_receivable=safe_get(bs_q, "Allowance For Doubtful Accounts Receivable", i),
            gross_accounts_receivable=safe_get(bs_q, "Gross Accounts Receivable", i),
            cash_cash_equivalents_and_short_term_investments=safe_get(bs_q, "Cash Cash Equivalents And Short Term Investments", i),
            other_short_term_investments=safe_get(bs_q, "Other Short Term Investments", i),
            cash=safe_get(bs_q, "Cash And Cash Equivalents", i),
            cash_equivalents=safe_get(bs_q, "Cash Equivalents", i),
            cash_financial=safe_get(bs_q, "Cash Financial", i),
        ))

        result["income_statement_quarterly"].append(IncomeStatementQuarterly(
            ticker=ticker,
            fiscal_quarter=fiscal_quarter,
            tax_effect_of_unusual_items=safe_get(inc_q, "Tax Effect Of Unusual Items", i),
            tax_rate_for_calcs=safe_get(inc_q, "Tax Rate For Calcs", i),
            normalized_ebitda=safe_get(inc_q, "Normalized EBITDA", i),
            total_unusual_items=safe_get(inc_q, "Total Unusual Items", i),
            total_unusual_items_excluding_goodwill=safe_get(inc_q, "Total Unusual Items Excluding Goodwill", i),
            net_income_from_continuing_operation_net_minority_interest=safe_get(inc_q, "Net Income From Continuing Operation Net Minority Interest", i),
            reconciled_depreciation=safe_get(inc_q, "Reconciled Depreciation", i),
            reconciled_cost_of_revenue=safe_get(inc_q, "Reconciled Cost Of Revenue", i),
            ebitda=safe_get(inc_q, "EBITDA", i),
            ebit=safe_get(inc_q, "EBIT", i),
            net_interest_income=safe_get(inc_q, "Net Interest Income", i),
            interest_expense=safe_get(inc_q, "Interest Expense", i),
            interest_income=safe_get(inc_q, "Interest Income", i),
            normalized_income=safe_get(inc_q, "Normalized Income", i),
            net_income_from_continuing_and_discontinued_operation=safe_get(inc_q, "Net Income From Continuing And Discontinued Operation", i),
            total_expenses=safe_get(inc_q, "Total Expenses", i),
            total_operating_income_as_reported=safe_get(inc_q, "Total Operating Income As Reported", i),
            diluted_average_shares=safe_get(inc_q, "Diluted Average Shares", i),
            basic_average_shares=safe_get(inc_q, "Basic Average Shares", i),
            diluted_eps=safe_get(inc_q, "Diluted EPS", i),
            basic_eps=safe_get(inc_q, "Basic EPS", i),
            diluted_ni_availto_com_stockholders=safe_get(inc_q, "Diluted NI Availto Com Stockholders", i),
            net_income_common_stockholders=safe_get(inc_q, "Net Income Common Stockholders", i),
            net_income=safe_get(inc_q, "Net Income", i),
            net_income_including_noncontrolling_interests=safe_get(inc_q, "Net Income Including Noncontrolling Interests", i),
            net_income_continuous_operations=safe_get(inc_q, "Net Income Continuous Operations", i),
            tax_provision=safe_get(inc_q, "Tax Provision", i),
            pretax_income=safe_get(inc_q, "Pretax Income", i),
            other_income_expense=safe_get(inc_q, "Other Income Expense", i),
            other_non_operating_income_expenses=safe_get(inc_q, "Other Non Operating Income Expenses", i),
            special_income_charges=safe_get(inc_q, "Special Income Charges", i),
            write_off=safe_get(inc_q, "Write Off", i),
            gain_on_sale_of_security=safe_get(inc_q, "Gain On Sale Of Security", i),
            net_non_operating_interest_income_expense=safe_get(inc_q, "Net Non Operating Interest Income Expense", i),
            interest_expense_non_operating=safe_get(inc_q, "Interest Expense Non Operating", i),
            interest_income_non_operating=safe_get(inc_q, "Interest Income Non Operating", i),
            operating_income=safe_get(inc_q, "Operating Income", i),
            operating_expense=safe_get(inc_q, "Operating Expense", i),
            research_and_development=safe_get(inc_q, "Research And Development", i),
            selling_general_and_administration=safe_get(inc_q, "Selling General And Administration", i),
            selling_and_marketing_expense=safe_get(inc_q, "Selling And Marketing Expense", i),
            general_and_administrative_expense=safe_get(inc_q, "General And Administrative Expense", i),
            other_g_and_a=safe_get(inc_q, "Other Gand A", i),
            gross_profit=safe_get(inc_q, "Gross Profit", i),
            cost_of_revenue=safe_get(inc_q, "Cost Of Revenue", i),
            total_revenue=safe_get(inc_q, "Total Revenue", i),
            operating_revenue=safe_get(inc_q, "Operating Revenue", i),
        ))

        result["cash_flow_quarterly"].append(CashFlowQuarterly(
            ticker=ticker,
            fiscal_quarter=fiscal_quarter,
            free_cash_flow=safe_get(cfs_q, "Free Cash Flow", i),
            repurchase_of_capital_stock=safe_get(cfs_q, "Repurchase Of Capital Stock", i),
            repayment_of_debt=safe_get(cfs_q, "Repayment Of Debt", i),
            issuance_of_debt=safe_get(cfs_q, "Issuance Of Debt", i),
            issuance_of_capital_stock=safe_get(cfs_q, "Issuance Of Capital Stock", i),
            capital_expenditure=safe_get(cfs_q, "Capital Expenditure", i),
            end_cash_position=safe_get(cfs_q, "End Cash Position", i),
            beginning_cash_position=safe_get(cfs_q, "Beginning Cash Position", i),
            effect_of_exchange_rate_changes=safe_get(cfs_q, "Effect Of Exchange Rate Changes", i),
            changes_in_cash=safe_get(cfs_q, "Changes In Cash", i),
            financing_cash_flow=safe_get(cfs_q, "Financing Cash Flow", i),
            cash_flow_from_continuing_financing_activities=safe_get(cfs_q, "Cash Flow From Continuing Financing Activities", i),
            net_other_financing_charges=safe_get(cfs_q, "Net Other Financing Charges", i),
            cash_dividends_paid=safe_get(cfs_q, "Cash Dividends Paid", i),
            common_stock_dividend_paid=safe_get(cfs_q, "Common Stock Dividend Paid", i),
            net_common_stock_issuance=safe_get(cfs_q, "Net Common Stock Issuance", i),
            common_stock_payments=safe_get(cfs_q, "Common Stock Payments", i),
            common_stock_issuance=safe_get(cfs_q, "Common Stock Issuance", i),
            net_issuance_payments_of_debt=safe_get(cfs_q, "Net Issuance Payments Of Debt", i),
            net_short_term_debt_issuance=safe_get(cfs_q, "Net Short Term Debt Issuance", i),
            short_term_debt_issuance=safe_get(cfs_q, "Short Term Debt Issuance", i),
            short_term_debt_payments=safe_get(cfs_q, "Short Term Debt Payments", i),
            net_long_term_debt_issuance=safe_get(cfs_q, "Net Long Term Debt Issuance", i),
            long_term_debt_payments=safe_get(cfs_q, "Long Term Debt Payments", i),
            long_term_debt_issuance=safe_get(cfs_q, "Long Term Debt Issuance", i),
            investing_cash_flow=safe_get(cfs_q, "Investing Cash Flow", i),
            cash_flow_from_continuing_investing_activities=safe_get(cfs_q, "Cash Flow From Continuing Investing Activities", i),
            net_other_investing_changes=safe_get(cfs_q, "Net Other Investing Changes", i),
            net_investment_purchase_and_sale=safe_get(cfs_q, "Net Investment Purchase And Sale", i),
            sale_of_investment=safe_get(cfs_q, "Sale Of Investment", i),
            purchase_of_investment=safe_get(cfs_q, "Purchase Of Investment", i),
            net_business_purchase_and_sale=safe_get(cfs_q, "Net Business Purchase And Sale", i),
            purchase_of_business=safe_get(cfs_q, "Purchase Of Business", i),
            net_ppe_purchase_and_sale=safe_get(cfs_q, "Net PPE Purchase And Sale", i),
            purchase_of_ppe=safe_get(cfs_q, "Purchase Of PPE", i),
            operating_cash_flow=safe_get(cfs_q, "Operating Cash Flow", i),
            cash_flow_from_continuing_operating_activities=safe_get(cfs_q, "Cash Flow From Continuing Operating Activities", i),
            change_in_working_capital=safe_get(cfs_q, "Change In Working Capital", i),
            change_in_other_working_capital=safe_get(cfs_q, "Change In Other Working Capital", i),
            change_in_other_current_liabilities=safe_get(cfs_q, "Change In Other Current Liabilities", i),
            change_in_other_current_assets=safe_get(cfs_q, "Change In Other Current Assets", i),
            change_in_payables_and_accrued_expense=safe_get(cfs_q, "Change In Payables And Accrued Expense", i),
            change_in_payable=safe_get(cfs_q, "Change In Payable", i),
            change_in_account_payable=safe_get(cfs_q, "Change In Account Payable", i),
            change_in_tax_payable=safe_get(cfs_q, "Change In Tax Payable", i),
            change_in_income_tax_payable=safe_get(cfs_q, "Change In Income Tax Payable", i),
            change_in_inventory=safe_get(cfs_q, "Change In Inventory", i),
            change_in_receivables=safe_get(cfs_q, "Change In Receivables", i),
            changes_in_account_receivables=safe_get(cfs_q, "Changes In Account Receivables", i),
            stock_based_compensation=safe_get(cfs_q, "Stock Based Compensation", i),
            unrealized_gain_loss_on_investment_securities=safe_get(cfs_q, "Unrealized Gain Loss On Investment Securities", i),
            asset_impairment_charge=safe_get(cfs_q, "Asset Impairment Charge", i),
            deferred_tax=safe_get(cfs_q, "Deferred Tax", i),
            deferred_income_tax=safe_get(cfs_q, "Deferred Income Tax", i),
            depreciation_amortization_depletion=safe_get(cfs_q, "Depreciation Amortization Depletion", i),
            depreciation_and_amortization=safe_get(cfs_q, "Depreciation And Amortization", i),
            depreciation=safe_get(cfs_q, "Depreciation", i),
            operating_gains_losses=safe_get(cfs_q, "Operating Gains Losses", i),
            gain_loss_on_investment_securities=safe_get(cfs_q, "Gain Loss On Investment Securities", i),
            net_income_from_continuing_operations=safe_get(cfs_q, "Net Income From Continuing Operations", i),
        ))

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
    try:
        response = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        data = pd.DataFrame.from_dict(response.json(), orient='index')
    except Exception as e:
        logger.error("Failed to fetch SEC company list: %s", e)
        return pd.DataFrame(columns=["ticker", "title", "cik_str"])
    data['cik_str'] = data['cik_str'].astype(str).str.zfill(10)
    if exclude:
        data = data[~data["ticker"].isin(exclude)]
    return data[["ticker", "title", "cik_str"]].head(limit)


def run_fetch(limit=25, exclude: set = None):
    companies = fetch_companies_from_sec(limit, exclude=exclude)
    results = []
    skipped = 0
    failed = 0

    rows = list(companies.itertuples(index=False))
    logger.info("Fetching %d tickers from SEC (excluded %d already processed)",
                len(rows), limit - len(rows) if len(rows) < limit else 0)
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        logger.info("Fetching batch %d–%d of %d", i + 1, i + len(batch), len(rows))
        for row in batch:
            try:
                r = fetch_ticker(row.ticker, row.title, row.cik_str)
                if r is not None:
                    results.append(r)
                else:
                    skipped += 1
            except Exception as e:
                failed += 1
                logger.error("Failed to fetch %s after retries: %s", row.ticker, e)
        if i + BATCH_SIZE < len(rows):
            time.sleep(DELAY_BETWEEN_BATCHES)

    logger.info("Fetch complete: %d succeeded, %d skipped (no data), %d failed",
                len(results), skipped, failed)
    return results

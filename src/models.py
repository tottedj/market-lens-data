from pydantic import BaseModel
from datetime import date


class Company(BaseModel):
    id: int | None = None
    ticker: str
    name: str | None
    cik: str | None
    sector: str | None = None
    industry: str | None = None
    country: str | None = None
    city: str | None = None
    state: str | None = None
    website: str | None = None
    description: str | None = None
    full_time_employees: int | None = None
    exchange: str | None = None
    currency: str | None = None
    quote_type: str | None = None


class _BalanceSheetBase(BaseModel):
    ticker: str
    # Equity
    ordinary_shares_number: float | None = None
    share_issued: float | None = None
    common_stock_equity: float | None = None
    stockholders_equity: float | None = None
    total_equity_gross_minority_interest: float | None = None
    retained_earnings: float | None = None
    capital_stock: float | None = None
    common_stock: float | None = None
    gains_losses_not_affecting_retained_earnings: float | None = None
    other_equity_adjustments: float | None = None
    # Debt & Capital
    net_debt: float | None = None
    total_debt: float | None = None
    invested_capital: float | None = None
    working_capital: float | None = None
    net_tangible_assets: float | None = None
    tangible_book_value: float | None = None
    capital_lease_obligations: float | None = None
    total_capitalization: float | None = None
    # Liabilities
    total_liabilities: float | None = None
    total_non_current_liabilities: float | None = None
    other_non_current_liabilities: float | None = None
    trade_and_other_payables_non_current: float | None = None
    non_current_deferred_liabilities: float | None = None
    non_current_deferred_revenue: float | None = None
    non_current_deferred_taxes_liabilities: float | None = None
    long_term_debt_and_capital_lease_obligation: float | None = None
    long_term_capital_lease_obligation: float | None = None
    long_term_debt: float | None = None
    current_liabilities: float | None = None
    other_current_liabilities: float | None = None
    current_deferred_liabilities: float | None = None
    current_deferred_revenue: float | None = None
    current_debt_and_capital_lease_obligation: float | None = None
    current_debt: float | None = None
    other_current_borrowings: float | None = None
    commercial_paper: float | None = None
    pension_and_other_post_retirement_benefit_plans_current: float | None = None
    payables_and_accrued_expenses: float | None = None
    payables: float | None = None
    total_tax_payable: float | None = None
    income_tax_payable: float | None = None
    accounts_payable: float | None = None
    # Assets
    total_assets: float | None = None
    total_non_current_assets: float | None = None
    other_non_current_assets: float | None = None
    financial_assets: float | None = None
    investments_and_advances: float | None = None
    investment_in_financial_assets: float | None = None
    available_for_sale_securities: float | None = None
    financial_assets_designated_fvtpl_total: float | None = None
    long_term_equity_investment: float | None = None
    goodwill_and_other_intangible_assets: float | None = None
    other_intangible_assets: float | None = None
    goodwill: float | None = None
    net_ppe: float | None = None
    accumulated_depreciation: float | None = None
    gross_ppe: float | None = None
    leases: float | None = None
    other_properties: float | None = None
    machinery_furniture_equipment: float | None = None
    buildings_and_improvements: float | None = None
    land_and_improvements: float | None = None
    properties: float | None = None
    current_assets: float | None = None
    other_current_assets: float | None = None
    hedging_assets_current: float | None = None
    inventory: float | None = None
    finished_goods: float | None = None
    work_in_process: float | None = None
    raw_materials: float | None = None
    receivables: float | None = None
    accounts_receivable: float | None = None
    allowance_for_doubtful_accounts_receivable: float | None = None
    gross_accounts_receivable: float | None = None
    cash_cash_equivalents_and_short_term_investments: float | None = None
    other_short_term_investments: float | None = None
    cash: float | None = None
    cash_equivalents: float | None = None
    cash_financial: float | None = None


class BalanceSheetAnnual(_BalanceSheetBase):
    fiscal_year: date


class BalanceSheetQuarterly(_BalanceSheetBase):
    fiscal_quarter: date


class _IncomeStatementBase(BaseModel):
    ticker: str
    tax_effect_of_unusual_items: float | None = None
    tax_rate_for_calcs: float | None = None
    normalized_ebitda: float | None = None
    total_unusual_items: float | None = None
    total_unusual_items_excluding_goodwill: float | None = None
    net_income_from_continuing_operation_net_minority_interest: float | None = None
    reconciled_depreciation: float | None = None
    reconciled_cost_of_revenue: float | None = None
    ebitda: float | None = None
    ebit: float | None = None
    net_interest_income: float | None = None
    interest_expense: float | None = None
    interest_income: float | None = None
    normalized_income: float | None = None
    net_income_from_continuing_and_discontinued_operation: float | None = None
    total_expenses: float | None = None
    total_operating_income_as_reported: float | None = None
    diluted_average_shares: float | None = None
    basic_average_shares: float | None = None
    diluted_eps: float | None = None
    basic_eps: float | None = None
    diluted_ni_availto_com_stockholders: float | None = None
    net_income_common_stockholders: float | None = None
    net_income: float | None = None
    net_income_including_noncontrolling_interests: float | None = None
    net_income_continuous_operations: float | None = None
    tax_provision: float | None = None
    pretax_income: float | None = None
    other_income_expense: float | None = None
    other_non_operating_income_expenses: float | None = None
    special_income_charges: float | None = None
    write_off: float | None = None
    gain_on_sale_of_security: float | None = None
    net_non_operating_interest_income_expense: float | None = None
    interest_expense_non_operating: float | None = None
    interest_income_non_operating: float | None = None
    operating_income: float | None = None
    operating_expense: float | None = None
    research_and_development: float | None = None
    selling_general_and_administration: float | None = None
    selling_and_marketing_expense: float | None = None
    general_and_administrative_expense: float | None = None
    other_g_and_a: float | None = None
    gross_profit: float | None = None
    cost_of_revenue: float | None = None
    total_revenue: float | None = None
    operating_revenue: float | None = None


class IncomeStatementAnnual(_IncomeStatementBase):
    fiscal_year: date


class IncomeStatementQuarterly(_IncomeStatementBase):
    fiscal_quarter: date


class _CashFlowBase(BaseModel):
    ticker: str
    free_cash_flow: float | None = None
    repurchase_of_capital_stock: float | None = None
    repayment_of_debt: float | None = None
    issuance_of_debt: float | None = None
    issuance_of_capital_stock: float | None = None
    capital_expenditure: float | None = None
    end_cash_position: float | None = None
    beginning_cash_position: float | None = None
    effect_of_exchange_rate_changes: float | None = None
    changes_in_cash: float | None = None
    financing_cash_flow: float | None = None
    cash_flow_from_continuing_financing_activities: float | None = None
    net_other_financing_charges: float | None = None
    cash_dividends_paid: float | None = None
    common_stock_dividend_paid: float | None = None
    net_common_stock_issuance: float | None = None
    common_stock_payments: float | None = None
    common_stock_issuance: float | None = None
    net_issuance_payments_of_debt: float | None = None
    net_short_term_debt_issuance: float | None = None
    short_term_debt_issuance: float | None = None
    short_term_debt_payments: float | None = None
    net_long_term_debt_issuance: float | None = None
    long_term_debt_payments: float | None = None
    long_term_debt_issuance: float | None = None
    investing_cash_flow: float | None = None
    cash_flow_from_continuing_investing_activities: float | None = None
    net_other_investing_changes: float | None = None
    net_investment_purchase_and_sale: float | None = None
    sale_of_investment: float | None = None
    purchase_of_investment: float | None = None
    net_business_purchase_and_sale: float | None = None
    purchase_of_business: float | None = None
    net_ppe_purchase_and_sale: float | None = None
    purchase_of_ppe: float | None = None
    operating_cash_flow: float | None = None
    cash_flow_from_continuing_operating_activities: float | None = None
    change_in_working_capital: float | None = None
    change_in_other_working_capital: float | None = None
    change_in_other_current_liabilities: float | None = None
    change_in_other_current_assets: float | None = None
    change_in_payables_and_accrued_expense: float | None = None
    change_in_payable: float | None = None
    change_in_account_payable: float | None = None
    change_in_tax_payable: float | None = None
    change_in_income_tax_payable: float | None = None
    change_in_inventory: float | None = None
    change_in_receivables: float | None = None
    changes_in_account_receivables: float | None = None
    stock_based_compensation: float | None = None
    unrealized_gain_loss_on_investment_securities: float | None = None
    asset_impairment_charge: float | None = None
    deferred_tax: float | None = None
    deferred_income_tax: float | None = None
    depreciation_amortization_depletion: float | None = None
    depreciation_and_amortization: float | None = None
    depreciation: float | None = None
    operating_gains_losses: float | None = None
    gain_loss_on_investment_securities: float | None = None
    net_income_from_continuing_operations: float | None = None


class CashFlowAnnual(_CashFlowBase):
    fiscal_year: date


class CashFlowQuarterly(_CashFlowBase):
    fiscal_quarter: date

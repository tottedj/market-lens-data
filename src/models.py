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


class BalanceSheetAnnual(BaseModel):
    ticker: str
    fiscal_year: date
    # Equity
    ordinary_shares_number: float | None
    share_issued: float | None
    common_stock_equity: float | None
    stockholders_equity: float | None
    total_equity_gross_minority_interest: float | None
    retained_earnings: float | None
    capital_stock: float | None
    common_stock: float | None
    gains_losses_not_affecting_retained_earnings: float | None
    other_equity_adjustments: float | None
    # Debt & Capital
    net_debt: float | None
    total_debt: float | None
    invested_capital: float | None
    working_capital: float | None
    net_tangible_assets: float | None
    tangible_book_value: float | None
    capital_lease_obligations: float | None
    total_capitalization: float | None
    # Liabilities
    total_liabilities: float | None
    total_non_current_liabilities: float | None
    other_non_current_liabilities: float | None
    trade_and_other_payables_non_current: float | None
    non_current_deferred_liabilities: float | None
    non_current_deferred_revenue: float | None
    non_current_deferred_taxes_liabilities: float | None
    long_term_debt_and_capital_lease_obligation: float | None
    long_term_capital_lease_obligation: float | None
    long_term_debt: float | None
    current_liabilities: float | None
    other_current_liabilities: float | None
    current_deferred_liabilities: float | None
    current_deferred_revenue: float | None
    current_debt_and_capital_lease_obligation: float | None
    current_debt: float | None
    other_current_borrowings: float | None
    commercial_paper: float | None
    pension_and_other_post_retirement_benefit_plans_current: float | None
    payables_and_accrued_expenses: float | None
    payables: float | None
    total_tax_payable: float | None
    income_tax_payable: float | None
    accounts_payable: float | None
    # Assets
    total_assets: float | None
    total_non_current_assets: float | None
    other_non_current_assets: float | None
    financial_assets: float | None
    investments_and_advances: float | None
    investment_in_financial_assets: float | None
    available_for_sale_securities: float | None
    financial_assets_designated_fvtpl_total: float | None
    long_term_equity_investment: float | None
    goodwill_and_other_intangible_assets: float | None
    other_intangible_assets: float | None
    goodwill: float | None
    net_ppe: float | None
    accumulated_depreciation: float | None
    gross_ppe: float | None
    leases: float | None
    other_properties: float | None
    machinery_furniture_equipment: float | None
    buildings_and_improvements: float | None
    land_and_improvements: float | None
    properties: float | None
    current_assets: float | None
    other_current_assets: float | None
    hedging_assets_current: float | None
    inventory: float | None
    finished_goods: float | None
    work_in_process: float | None
    raw_materials: float | None
    receivables: float | None
    accounts_receivable: float | None
    allowance_for_doubtful_accounts_receivable: float | None
    gross_accounts_receivable: float | None
    cash_cash_equivalents_and_short_term_investments: float | None
    other_short_term_investments: float | None
    cash: float | None
    cash_equivalents: float | None
    cash_financial: float | None


class IncomeStatementAnnual(BaseModel):
    ticker: str
    fiscal_year: date
    tax_effect_of_unusual_items: float | None
    tax_rate_for_calcs: float | None
    normalized_ebitda: float | None
    total_unusual_items: float | None
    total_unusual_items_excluding_goodwill: float | None
    net_income_from_continuing_operation_net_minority_interest: float | None
    reconciled_depreciation: float | None
    reconciled_cost_of_revenue: float | None
    ebitda: float | None
    ebit: float | None
    net_interest_income: float | None
    interest_expense: float | None
    interest_income: float | None
    normalized_income: float | None
    net_income_from_continuing_and_discontinued_operation: float | None
    total_expenses: float | None
    total_operating_income_as_reported: float | None
    diluted_average_shares: float | None
    basic_average_shares: float | None
    diluted_eps: float | None
    basic_eps: float | None
    diluted_ni_availto_com_stockholders: float | None
    net_income_common_stockholders: float | None
    net_income: float | None
    net_income_including_noncontrolling_interests: float | None
    net_income_continuous_operations: float | None
    tax_provision: float | None
    pretax_income: float | None
    other_income_expense: float | None
    other_non_operating_income_expenses: float | None
    special_income_charges: float | None
    write_off: float | None
    gain_on_sale_of_security: float | None
    net_non_operating_interest_income_expense: float | None
    interest_expense_non_operating: float | None
    interest_income_non_operating: float | None
    operating_income: float | None
    operating_expense: float | None
    research_and_development: float | None
    selling_general_and_administration: float | None
    selling_and_marketing_expense: float | None
    general_and_administrative_expense: float | None
    other_g_and_a: float | None
    gross_profit: float | None
    cost_of_revenue: float | None
    total_revenue: float | None
    operating_revenue: float | None


class CashFlowAnnual(BaseModel):
    ticker: str
    fiscal_year: date
    free_cash_flow: float | None
    repurchase_of_capital_stock: float | None
    repayment_of_debt: float | None
    issuance_of_debt: float | None
    issuance_of_capital_stock: float | None
    capital_expenditure: float | None
    end_cash_position: float | None
    beginning_cash_position: float | None
    effect_of_exchange_rate_changes: float | None
    changes_in_cash: float | None
    financing_cash_flow: float | None
    cash_flow_from_continuing_financing_activities: float | None
    net_other_financing_charges: float | None
    cash_dividends_paid: float | None
    common_stock_dividend_paid: float | None
    net_common_stock_issuance: float | None
    common_stock_payments: float | None
    common_stock_issuance: float | None
    net_issuance_payments_of_debt: float | None
    net_short_term_debt_issuance: float | None
    short_term_debt_issuance: float | None
    short_term_debt_payments: float | None
    net_long_term_debt_issuance: float | None
    long_term_debt_payments: float | None
    long_term_debt_issuance: float | None
    investing_cash_flow: float | None
    cash_flow_from_continuing_investing_activities: float | None
    net_other_investing_changes: float | None
    net_investment_purchase_and_sale: float | None
    sale_of_investment: float | None
    purchase_of_investment: float | None
    net_business_purchase_and_sale: float | None
    purchase_of_business: float | None
    net_ppe_purchase_and_sale: float | None
    purchase_of_ppe: float | None
    operating_cash_flow: float | None
    cash_flow_from_continuing_operating_activities: float | None
    change_in_working_capital: float | None
    change_in_other_working_capital: float | None
    change_in_other_current_liabilities: float | None
    change_in_other_current_assets: float | None
    change_in_payables_and_accrued_expense: float | None
    change_in_payable: float | None
    change_in_account_payable: float | None
    change_in_tax_payable: float | None
    change_in_income_tax_payable: float | None
    change_in_inventory: float | None
    change_in_receivables: float | None
    changes_in_account_receivables: float | None
    stock_based_compensation: float | None
    unrealized_gain_loss_on_investment_securities: float | None
    asset_impairment_charge: float | None
    deferred_tax: float | None
    deferred_income_tax: float | None
    depreciation_amortization_depletion: float | None
    depreciation_and_amortization: float | None
    depreciation: float | None
    operating_gains_losses: float | None
    gain_loss_on_investment_securities: float | None
    net_income_from_continuing_operations: float | None


class BalanceSheetQuarterly(BaseModel):
    ticker: str
    fiscal_quarter: date
    # Equity
    ordinary_shares_number: float | None
    share_issued: float | None
    common_stock_equity: float | None
    stockholders_equity: float | None
    total_equity_gross_minority_interest: float | None
    retained_earnings: float | None
    capital_stock: float | None
    common_stock: float | None
    gains_losses_not_affecting_retained_earnings: float | None
    other_equity_adjustments: float | None
    # Debt & Capital
    net_debt: float | None
    total_debt: float | None
    invested_capital: float | None
    working_capital: float | None
    net_tangible_assets: float | None
    tangible_book_value: float | None
    capital_lease_obligations: float | None
    total_capitalization: float | None
    # Liabilities
    total_liabilities: float | None
    total_non_current_liabilities: float | None
    other_non_current_liabilities: float | None
    trade_and_other_payables_non_current: float | None
    non_current_deferred_liabilities: float | None
    non_current_deferred_revenue: float | None
    non_current_deferred_taxes_liabilities: float | None
    long_term_debt_and_capital_lease_obligation: float | None
    long_term_capital_lease_obligation: float | None
    long_term_debt: float | None
    current_liabilities: float | None
    other_current_liabilities: float | None
    current_deferred_liabilities: float | None
    current_deferred_revenue: float | None
    current_debt_and_capital_lease_obligation: float | None
    current_debt: float | None
    other_current_borrowings: float | None
    commercial_paper: float | None
    pension_and_other_post_retirement_benefit_plans_current: float | None
    payables_and_accrued_expenses: float | None
    payables: float | None
    total_tax_payable: float | None
    income_tax_payable: float | None
    accounts_payable: float | None
    # Assets
    total_assets: float | None
    total_non_current_assets: float | None
    other_non_current_assets: float | None
    financial_assets: float | None
    investments_and_advances: float | None
    investment_in_financial_assets: float | None
    available_for_sale_securities: float | None
    financial_assets_designated_fvtpl_total: float | None
    long_term_equity_investment: float | None
    goodwill_and_other_intangible_assets: float | None
    other_intangible_assets: float | None
    goodwill: float | None
    net_ppe: float | None
    accumulated_depreciation: float | None
    gross_ppe: float | None
    leases: float | None
    other_properties: float | None
    machinery_furniture_equipment: float | None
    buildings_and_improvements: float | None
    land_and_improvements: float | None
    properties: float | None
    current_assets: float | None
    other_current_assets: float | None
    hedging_assets_current: float | None
    inventory: float | None
    finished_goods: float | None
    work_in_process: float | None
    raw_materials: float | None
    receivables: float | None
    accounts_receivable: float | None
    allowance_for_doubtful_accounts_receivable: float | None
    gross_accounts_receivable: float | None
    cash_cash_equivalents_and_short_term_investments: float | None
    other_short_term_investments: float | None
    cash: float | None
    cash_equivalents: float | None
    cash_financial: float | None


class IncomeStatementQuarterly(BaseModel):
    ticker: str
    fiscal_quarter: date
    tax_effect_of_unusual_items: float | None
    tax_rate_for_calcs: float | None
    normalized_ebitda: float | None
    total_unusual_items: float | None
    total_unusual_items_excluding_goodwill: float | None
    net_income_from_continuing_operation_net_minority_interest: float | None
    reconciled_depreciation: float | None
    reconciled_cost_of_revenue: float | None
    ebitda: float | None
    ebit: float | None
    net_interest_income: float | None
    interest_expense: float | None
    interest_income: float | None
    normalized_income: float | None
    net_income_from_continuing_and_discontinued_operation: float | None
    total_expenses: float | None
    total_operating_income_as_reported: float | None
    diluted_average_shares: float | None
    basic_average_shares: float | None
    diluted_eps: float | None
    basic_eps: float | None
    diluted_ni_availto_com_stockholders: float | None
    net_income_common_stockholders: float | None
    net_income: float | None
    net_income_including_noncontrolling_interests: float | None
    net_income_continuous_operations: float | None
    tax_provision: float | None
    pretax_income: float | None
    other_income_expense: float | None
    other_non_operating_income_expenses: float | None
    special_income_charges: float | None
    write_off: float | None
    gain_on_sale_of_security: float | None
    net_non_operating_interest_income_expense: float | None
    interest_expense_non_operating: float | None
    interest_income_non_operating: float | None
    operating_income: float | None
    operating_expense: float | None
    research_and_development: float | None
    selling_general_and_administration: float | None
    selling_and_marketing_expense: float | None
    general_and_administrative_expense: float | None
    other_g_and_a: float | None
    gross_profit: float | None
    cost_of_revenue: float | None
    total_revenue: float | None
    operating_revenue: float | None


class CashFlowQuarterly(BaseModel):
    ticker: str
    fiscal_quarter: date
    free_cash_flow: float | None
    repurchase_of_capital_stock: float | None
    repayment_of_debt: float | None
    issuance_of_debt: float | None
    issuance_of_capital_stock: float | None
    capital_expenditure: float | None
    end_cash_position: float | None
    beginning_cash_position: float | None
    effect_of_exchange_rate_changes: float | None
    changes_in_cash: float | None
    financing_cash_flow: float | None
    cash_flow_from_continuing_financing_activities: float | None
    net_other_financing_charges: float | None
    cash_dividends_paid: float | None
    common_stock_dividend_paid: float | None
    net_common_stock_issuance: float | None
    common_stock_payments: float | None
    common_stock_issuance: float | None
    net_issuance_payments_of_debt: float | None
    net_short_term_debt_issuance: float | None
    short_term_debt_issuance: float | None
    short_term_debt_payments: float | None
    net_long_term_debt_issuance: float | None
    long_term_debt_payments: float | None
    long_term_debt_issuance: float | None
    investing_cash_flow: float | None
    cash_flow_from_continuing_investing_activities: float | None
    net_other_investing_changes: float | None
    net_investment_purchase_and_sale: float | None
    sale_of_investment: float | None
    purchase_of_investment: float | None
    net_business_purchase_and_sale: float | None
    purchase_of_business: float | None
    net_ppe_purchase_and_sale: float | None
    purchase_of_ppe: float | None
    operating_cash_flow: float | None
    cash_flow_from_continuing_operating_activities: float | None
    change_in_working_capital: float | None
    change_in_other_working_capital: float | None
    change_in_other_current_liabilities: float | None
    change_in_other_current_assets: float | None
    change_in_payables_and_accrued_expense: float | None
    change_in_payable: float | None
    change_in_account_payable: float | None
    change_in_tax_payable: float | None
    change_in_income_tax_payable: float | None
    change_in_inventory: float | None
    change_in_receivables: float | None
    changes_in_account_receivables: float | None
    stock_based_compensation: float | None
    unrealized_gain_loss_on_investment_securities: float | None
    asset_impairment_charge: float | None
    deferred_tax: float | None
    deferred_income_tax: float | None
    depreciation_amortization_depletion: float | None
    depreciation_and_amortization: float | None
    depreciation: float | None
    operating_gains_losses: float | None
    gain_loss_on_investment_securities: float | None
    net_income_from_continuing_operations: float | None

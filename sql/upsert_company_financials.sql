-- Run this in the Supabase SQL Editor to create the RPC function.
-- It wraps the entire company + financials upsert in a single transaction.
-- If any part fails, everything rolls back automatically.

CREATE OR REPLACE FUNCTION upsert_company_financials(payload jsonb)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  v_company_id integer;
BEGIN
  -- 1. Upsert company
  INSERT INTO companies (ticker, name, cik)
  VALUES (
    payload->>'ticker',
    payload->>'name',
    payload->>'cik'
  )
  ON CONFLICT (ticker) DO UPDATE SET
    name = EXCLUDED.name,
    cik = EXCLUDED.cik
  RETURNING id INTO v_company_id;

  -- 2. Balance sheet annual
  IF payload->'balance_sheet_annual' IS NOT NULL
     AND jsonb_array_length(payload->'balance_sheet_annual') > 0 THEN
    DELETE FROM balance_sheet_annual
    WHERE company_id = v_company_id
      AND fiscal_year IN (
        SELECT (r->>'fiscal_year')::date
        FROM jsonb_array_elements(payload->'balance_sheet_annual') AS r
      );
    INSERT INTO balance_sheet_annual
    SELECT (jsonb_populate_record(
      null::balance_sheet_annual,
      r || jsonb_build_object('company_id', v_company_id)
    )).*
    FROM jsonb_array_elements(payload->'balance_sheet_annual') AS r;
  END IF;

  -- 3. Income statement annual
  IF payload->'income_statement_annual' IS NOT NULL
     AND jsonb_array_length(payload->'income_statement_annual') > 0 THEN
    DELETE FROM income_statement_annual
    WHERE company_id = v_company_id
      AND fiscal_year IN (
        SELECT (r->>'fiscal_year')::date
        FROM jsonb_array_elements(payload->'income_statement_annual') AS r
      );
    INSERT INTO income_statement_annual
    SELECT (jsonb_populate_record(
      null::income_statement_annual,
      r || jsonb_build_object('company_id', v_company_id)
    )).*
    FROM jsonb_array_elements(payload->'income_statement_annual') AS r;
  END IF;

  -- 4. Cash flow annual
  IF payload->'cash_flow_annual' IS NOT NULL
     AND jsonb_array_length(payload->'cash_flow_annual') > 0 THEN
    DELETE FROM cash_flow_annual
    WHERE company_id = v_company_id
      AND fiscal_year IN (
        SELECT (r->>'fiscal_year')::date
        FROM jsonb_array_elements(payload->'cash_flow_annual') AS r
      );
    INSERT INTO cash_flow_annual
    SELECT (jsonb_populate_record(
      null::cash_flow_annual,
      r || jsonb_build_object('company_id', v_company_id)
    )).*
    FROM jsonb_array_elements(payload->'cash_flow_annual') AS r;
  END IF;

  -- 5. Balance sheet quarterly
  IF payload->'balance_sheet_quarterly' IS NOT NULL
     AND jsonb_array_length(payload->'balance_sheet_quarterly') > 0 THEN
    DELETE FROM balance_sheet_quarterly
    WHERE company_id = v_company_id
      AND fiscal_quarter IN (
        SELECT (r->>'fiscal_quarter')::date
        FROM jsonb_array_elements(payload->'balance_sheet_quarterly') AS r
      );
    INSERT INTO balance_sheet_quarterly
    SELECT (jsonb_populate_record(
      null::balance_sheet_quarterly,
      r || jsonb_build_object('company_id', v_company_id)
    )).*
    FROM jsonb_array_elements(payload->'balance_sheet_quarterly') AS r;
  END IF;

  -- 6. Income statement quarterly
  IF payload->'income_statement_quarterly' IS NOT NULL
     AND jsonb_array_length(payload->'income_statement_quarterly') > 0 THEN
    DELETE FROM income_statement_quarterly
    WHERE company_id = v_company_id
      AND fiscal_quarter IN (
        SELECT (r->>'fiscal_quarter')::date
        FROM jsonb_array_elements(payload->'income_statement_quarterly') AS r
      );
    INSERT INTO income_statement_quarterly
    SELECT (jsonb_populate_record(
      null::income_statement_quarterly,
      r || jsonb_build_object('company_id', v_company_id)
    )).*
    FROM jsonb_array_elements(payload->'income_statement_quarterly') AS r;
  END IF;

  -- 7. Cash flow quarterly
  IF payload->'cash_flow_quarterly' IS NOT NULL
     AND jsonb_array_length(payload->'cash_flow_quarterly') > 0 THEN
    DELETE FROM cash_flow_quarterly
    WHERE company_id = v_company_id
      AND fiscal_quarter IN (
        SELECT (r->>'fiscal_quarter')::date
        FROM jsonb_array_elements(payload->'cash_flow_quarterly') AS r
      );
    INSERT INTO cash_flow_quarterly
    SELECT (jsonb_populate_record(
      null::cash_flow_quarterly,
      r || jsonb_build_object('company_id', v_company_id)
    )).*
    FROM jsonb_array_elements(payload->'cash_flow_quarterly') AS r;
  END IF;

  RETURN v_company_id;
END;
$$;

CREATE TABLE skipped_tickers (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    reason TEXT,
    skipped_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE skipped_tickers IS 'Tickers that failed validation and should be excluded from future pipeline runs.';

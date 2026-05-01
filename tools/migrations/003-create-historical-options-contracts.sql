-- 003 — historical_options_contracts hypertable.
-- Per-as-of-date contract universe. One row per (as_of_date, ticker).
CREATE TABLE IF NOT EXISTS historical_options_contracts (
  as_of_date DATE NOT NULL,
  ticker VARCHAR(50) NOT NULL,
  underlying_ticker VARCHAR(10) NOT NULL,
  contract_type VARCHAR(10),
  exercise_style VARCHAR(20),
  expiration_date DATE,
  strike_price DECIMAL(18,4),
  shares_per_contract INT,
  primary_exchange VARCHAR(10)
);

SELECT create_hypertable('historical_options_contracts', 'as_of_date', if_not_exists => TRUE);
CREATE UNIQUE INDEX IF NOT EXISTS uq_options_date_ticker
  ON historical_options_contracts (as_of_date, ticker);
CREATE INDEX IF NOT EXISTS idx_options_underlying
  ON historical_options_contracts (underlying_ticker, as_of_date DESC);

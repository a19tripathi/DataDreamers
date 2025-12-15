CREATE TABLE IF NOT EXISTS `analytics.fact_loan_snapshot` (
  loan_id INT64,
  borrower_id INT64,
  facility_id INT64,
  snapshot_date_key INT64,
  snapshot_date DATE,
  outstanding_principal NUMERIC,
  current_rate_pct NUMERIC,
  margin_bps INT64,
  rating_grade STRING,
  score NUMERIC
);

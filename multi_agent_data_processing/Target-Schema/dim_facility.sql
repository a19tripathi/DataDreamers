CREATE TABLE IF NOT EXISTS `analytics.dim_facility` (
  facility_id INT64,
  borrower_id INT64,
  facility_type STRING,
  limit_amount NUMERIC,
  currency STRING,
  origination_date DATE,
  maturity_date DATE,
  interest_rate_floor_bps INT64,
  covenants_count INT64
);

CREATE TABLE IF NOT EXISTS `analytics.dim_collateral` (
  collateral_id INT64,
  loan_id INT64,
  collateral_type STRING,
  value_amount NUMERIC,
  currency STRING,
  valuation_date DATE,
  lien_position STRING,
  location_country STRING,
  location_state STRING
);

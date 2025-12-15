CREATE TABLE IF NOT EXISTS `analytics.dim_guarantor` (
  guarantor_id INT64,
  borrower_id INT64,
  guarantor_name STRING,
  guarantor_type STRING,
  guarantee_type STRING,
  max_liability_amount NUMERIC,
  currency STRING,
  credit_score INT64,
  ownership_pct NUMERIC
);

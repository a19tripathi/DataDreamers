CREATE TABLE IF NOT EXISTS `analytics.dim_borrower` (
  borrower_id INT64,
  borrower_name STRING,
  borrower_type STRING,
  industry STRING,
  tax_id STRING,
  country STRING,
  state STRING,
  city STRING,
  postal_code STRING,
  inception_date DATE,
  annual_revenue NUMERIC,
  employees INT64
);

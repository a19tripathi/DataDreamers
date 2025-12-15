CREATE TABLE IF NOT EXISTS `analytics.fact_payments` (
  payment_id INT64,
  date_key INT64,
  payment_date DATE,
  loan_id INT64,
  borrower_id INT64,
  facility_id INT64,
  index_id INT64,
  index_name STRING,
  tenor_months INT64,
  payment_amount NUMERIC,
  principal_component NUMERIC,
  interest_component NUMERIC,
  fee_component NUMERIC,
  days_past_due INT64,
  currency STRING,
  payment_method STRING,
  margin_bps INT64
);

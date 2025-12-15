CREATE TABLE IF NOT EXISTS `analytics.dim_risk_rating` (
  rating_id INT64,
  loan_id INT64,
  borrower_id INT64,
  rating_agency STRING,
  rating_grade STRING,
  score NUMERIC,
  effective_date DATE,
  expiry_date DATE
);

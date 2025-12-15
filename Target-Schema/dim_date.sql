CREATE SCHEMA IF NOT EXISTS `analytics`;
CREATE TABLE IF NOT EXISTS `analytics.dim_date` (
  date_key INT64,
  date DATE,
  year INT64,
  quarter STRING,
  month INT64,
  day INT64
);

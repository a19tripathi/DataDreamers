CREATE TABLE IF NOT EXISTS `analytics.dim_rate_index` (
  index_id INT64,
  index_name STRING,
  tenor_months INT64,
  index_currency STRING,
  rate_type STRING,
  day_count_convention STRING,
  published_by STRING
);

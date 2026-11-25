-- Gold: business marts. Materialized views over silver; serverless refreshes them incrementally when it can.
-- Keep these queries deterministic (no current_date()/now()): a time function forces a full recompute.

CREATE OR REFRESH MATERIALIZED VIEW ${gold}.customer_360
COMMENT 'One row per customer with lifetime purchase metrics'
AS WITH order_stats AS (
  SELECT
    customer_id,
    min(order_date)                                                  AS first_order_date,
    max(order_date)                                                  AS last_order_date,
    count(DISTINCT order_id) FILTER (WHERE NOT is_cancelled)         AS order_count,
    sum(net_amount_eur) FILTER (WHERE NOT is_cancelled)              AS lifetime_value_eur
  FROM ${silver}.orders
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.full_name,
  c.country_code,
  c.loyalty_tier,
  s.first_order_date,
  s.last_order_date,
  coalesce(s.order_count, 0)                                         AS order_count,
  coalesce(s.lifetime_value_eur, 0)                                  AS lifetime_value_eur,
  round(s.lifetime_value_eur / nullif(s.order_count, 0), 2)          AS avg_order_value_eur,
  datediff(s.last_order_date, c.signup_date)                         AS active_span_days
FROM ${silver}.customers c
LEFT JOIN order_stats s ON s.customer_id = c.customer_id;

CREATE OR REFRESH MATERIALIZED VIEW ${gold}.daily_sales (
  order_date        DATE           COMMENT 'Calendar day the order was placed (UTC)',
  category          STRING         COMMENT 'Product category',
  country_code      STRING         COMMENT 'Customer country (ISO 3166-1 alpha-2)',
  orders            BIGINT,
  units             BIGINT,
  revenue_eur       DECIMAL(18, 2),
  gross_margin_eur  DECIMAL(18, 2) COMMENT 'Revenue before discounts',
  report_currency   STRING
)
COMMENT 'Daily sales by product category and customer country, excluding cancelled and returned orders'
AS SELECT
  o.order_date,
  p.category,
  c.country_code,
  count(DISTINCT o.order_id)                                         AS orders,
  CAST(sum(o.quantity) AS BIGINT)                                    AS units,
  CAST(sum(o.net_amount_eur) AS DECIMAL(18, 2))                      AS revenue_eur,
  CAST(sum(o.net_amount_eur - o.quantity * p.cost_price_eur) AS DECIMAL(18, 2))
                                                                     AS gross_margin_eur,
  'EUR'                                                              AS report_currency
FROM ${silver}.orders o
JOIN ${silver}.products p ON p.sku = o.sku
JOIN ${silver}.customers c ON c.customer_id = o.customer_id
WHERE NOT o.is_cancelled
GROUP BY o.order_date, p.category, c.country_code;

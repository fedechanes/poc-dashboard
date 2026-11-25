-- Silver: cleaned, typed and conformed. Dedup happens here.

-- Customers, step 1: cleanse every exported record (append-only, incremental stream from bronze).
CREATE OR REFRESH STREAMING TABLE ${silver}.customers_cleansed (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Typed and standardised CRM records, still one row per export'
AS SELECT
  CAST(regexp_replace(cust_id, '^C', '') AS BIGINT)                  AS customer_id,
  concat_ws(' ', initcap(trim(first_nm)), initcap(trim(last_nm)))    AS full_name,
  lower(trim(email_addr))                                            AS email,
  upper(trim(country_cd))                                            AS country_code,
  to_date(signup_dt)                                                 AS signup_date,
  CASE loyalty_tier WHEN 'G' THEN 'Gold' WHEN 'S' THEN 'Silver' WHEN 'B' THEN 'Bronze' ELSE 'None' END
                                                                     AS loyalty_tier,
  to_timestamp(updated_at)                                           AS updated_at
FROM STREAM(${bronze}.customers_raw);

-- Customers, step 2: one current row per customer (SCD type 1 upsert, latest updated_at wins).
CREATE OR REFRESH STREAMING TABLE ${silver}.customers
COMMENT 'Current state of each customer (deduplicated CRM records)';

CREATE FLOW customers_upsert AS AUTO CDC INTO ${silver}.customers
FROM STREAM(${silver}.customers_cleansed)
KEYS (customer_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;

-- Products: latest catalogue row per SKU, typed, with a derived margin.
CREATE OR REFRESH MATERIALIZED VIEW ${silver}.products
COMMENT 'Current product catalogue, one row per SKU'
AS SELECT
  upper(trim(sku))                                                   AS sku,
  product_name,
  lower(category)                                                    AS category,
  CAST(list_price AS DECIMAL(10, 2))                                 AS list_price_eur,
  CAST(cost_price AS DECIMAL(10, 2))                                 AS cost_price_eur,
  round((CAST(list_price AS DECIMAL(10, 2)) - CAST(cost_price AS DECIMAL(10, 2)))
        / CAST(list_price AS DECIMAL(10, 2)) * 100, 1)               AS margin_pct
FROM ${bronze}.products_raw
QUALIFY row_number() OVER (PARTITION BY upper(trim(sku)) ORDER BY updated_at DESC) = 1;

-- Orders: latest event per order, typed, amounts converted to EUR.
CREATE OR REFRESH MATERIALIZED VIEW ${silver}.orders
COMMENT 'One row per order with its latest status, amounts in EUR'
AS WITH latest AS (
  SELECT *
  FROM ${bronze}.orders_raw
  QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY CAST(event_ts AS BIGINT) DESC) = 1
), typed AS (
  SELECT
    CAST(order_id AS BIGINT)                                         AS order_id,
    CAST(regexp_replace(cust_id, '^C', '') AS BIGINT)                AS customer_id,
    timestamp_millis(CAST(order_ts AS BIGINT))                       AS order_ts,
    upper(trim(sku))                                                 AS sku,
    CAST(qty AS INT)                                                 AS quantity,
    upper(currency)                                                  AS currency_code,
    CAST(qty AS INT) * CAST(unit_price AS DECIMAL(10, 2))
      - coalesce(CAST(discount_amt AS DECIMAL(10, 2)), 0)            AS net_amount,
    CASE upper(currency) WHEN 'USD' THEN 0.926 WHEN 'GBP' THEN 1.163 ELSE 1.0 END
                                                                     AS fx_rate_to_eur,
    lower(status)                                                    AS order_status
  FROM latest
)
SELECT
  order_id,
  customer_id,
  order_ts,
  to_date(order_ts)                                                  AS order_date,
  sku,
  quantity,
  currency_code,
  net_amount,
  CAST(round(net_amount * fx_rate_to_eur, 2) AS DECIMAL(12, 2))      AS net_amount_eur,
  order_status,
  order_status IN ('cancelled', 'returned')                          AS is_cancelled
FROM typed;

-- Bronze: raw extracts as landed, ingested incrementally with Auto Loader (read_files on a stream).
-- Each run picks up only files not seen before (checkpointed by the streaming table).
-- Everything stays a string (inferColumnTypes => false): typing happens in silver.

CREATE OR REFRESH STREAMING TABLE ${bronze}.customers_raw
COMMENT 'CRM customer export, one row per exported record, as landed'
AS SELECT *,
       _metadata.file_name AS _source_file,
       current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing}/customers', format => 'json', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE ${bronze}.orders_raw
COMMENT 'Web-shop order events (one per status change), as landed'
AS SELECT *,
       _metadata.file_name AS _source_file,
       current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing}/orders', format => 'json', inferColumnTypes => false);

CREATE OR REFRESH STREAMING TABLE ${bronze}.products_raw
COMMENT 'Product catalogue extract, re-sent rows on price changes, as landed'
AS SELECT *,
       _metadata.file_name AS _source_file,
       current_timestamp() AS _ingested_at
FROM STREAM read_files('${landing}/products', format => 'csv', header => true, inferColumnTypes => false);

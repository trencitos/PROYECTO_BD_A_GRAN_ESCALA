WITH raw_sales AS (
    SELECT 
        id AS transaction_id,
        store AS store_id,
        (financials).raw_amount AS amount,
        (financials).currency AS currency,
        status_history
    FROM {{ source('silver', 'sales_enriched') }}
)
SELECT * FROM raw_sales
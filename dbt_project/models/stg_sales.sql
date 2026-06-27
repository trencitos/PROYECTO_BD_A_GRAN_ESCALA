WITH raw_sales AS (
    SELECT 
        id AS transaction_id,
        store AS store_id,
        (financials->>'raw_amount')::numeric AS amount,
        financials->>'currency' AS currency,
        status_history,
        (metadata->>'processed_at')::timestamp AS pipeline_processed_at,
        metadata->>'batch_id' AS batch_id,
        (metadata->>'is_active')::boolean AS is_active_transaction
    FROM {{ source('silver', 'sales_enriched') }}
)
SELECT * FROM raw_sales
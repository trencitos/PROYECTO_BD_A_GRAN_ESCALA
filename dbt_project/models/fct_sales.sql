{{ config(
    materialized='table'
) }}

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),
exchange_rates AS (
    SELECT * FROM {{ ref('ref_exchange_rates') }}
),
status_times AS (
    SELECT 
        transaction_id,
        MIN(CASE WHEN status = 'CREATED' THEN date END) as created_at,
        MAX(CASE WHEN status = 'COMPLETED' THEN date END) as completed_at,
        MAX(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) as is_refunded
    FROM sales, json_to_recordset(sales.status_history) as sh(status text, date timestamp)
    GROUP BY transaction_id
)

SELECT 
    s.transaction_id,
    s.store_id,
    s.amount,
    s.currency,
    (s.amount / e.rate_to_usd) AS amount_usd,
    st.created_at,
    st.completed_at,
    EXTRACT(EPOCH FROM (st.completed_at - st.created_at))/3600 AS conversion_time_hours,
    st.is_refunded,
    s.is_active_transaction,
    s.pipeline_processed_at,
    s.batch_id,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM sales s
LEFT JOIN exchange_rates e ON s.currency = e.currency
LEFT JOIN status_times st ON s.transaction_id = st.transaction_id
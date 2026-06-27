WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),
exchange_rates AS (
    SELECT * FROM {{ ref('ref_exchange_rates') }}
),
-- Calculamos la lógica del tiempo de conversión si aplica [cite: 63]
status_times AS (
    SELECT 
        transaction_id,
        MIN(CASE WHEN status = 'CREATED' THEN date END) as created_at,
        MAX(CASE WHEN status = 'COMPLETED' THEN date END) as completed_at,
        MAX(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END) as is_refunded
    FROM sales, UNNEST(status_history) sh(status, date)
    GROUP BY transaction_id
)

SELECT 
    s.transaction_id,
    s.store_id,
    s.amount,
    s.currency,
    (s.amount / e.rate_to_usd) AS amount_usd, -- Normalización a USD [cite: 60]
    st.created_at,
    st.completed_at,
    EXTRACT(EPOCH FROM (st.completed_at - st.created_at))/3600 AS conversion_time_hours,
    st.is_refunded
FROM sales s
LEFT JOIN exchange_rates e ON s.currency = e.currency
LEFT JOIN status_times st ON s.transaction_id = st.transaction_id
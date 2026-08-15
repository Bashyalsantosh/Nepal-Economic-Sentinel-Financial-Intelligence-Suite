{{ config(
    materialized='view',
    schema='silver'
) }}

WITH source_data AS (
    SELECT
        contract_id,
        trade_date,
        UPPER(TRIM(symbol)) AS symbol,
        buyer_broker,
        seller_broker,
        quantity,
        rate,
        total_amount,
        processed_at
    FROM {{ source('silver', 'nepse_floorsheet_trades') }}
    WHERE trade_date IS NOT NULL
      AND quantity > 0
      AND total_amount > 0
)

SELECT * FROM source_data


{{ config(
    materialized='incremental',
    schema='gold',
    unique_key=['trade_date', 'buyer_broker', 'seller_broker', 'symbol'],
    incremental_strategy='merge'
) }}

WITH daily_aggregates AS (
    SELECT
        trade_date,
        buyer_broker,
        seller_broker,
        symbol,
        SUM(quantity)::BIGINT AS total_volume,
        SUM(total_amount)::NUMERIC(18, 2) AS total_amount,
        COUNT(contract_id)::INTEGER AS trade_count,
        CURRENT_TIMESTAMP AS created_at
    FROM {{ ref('stg_nepse_floorsheet') }}

    {% if is_incremental() %}
        -- Only process new or updated dates during daily DAG runs
        WHERE trade_date >= (SELECT MAX(trade_date) FROM {{ this }})
    {% endif %}

    GROUP BY 
        trade_date,
        buyer_broker,
        seller_broker,
        symbol
)

SELECT * FROM daily_aggregates






{{ config(
    materialized='view',
    schema='gold'
) }}

WITH broker_pairs AS (
    SELECT
        trade_date,
        symbol,
        buyer_broker AS broker_a,
        seller_broker AS broker_b,
        total_volume AS volume_a_to_b,
        total_amount AS amount_a_to_b,
        trade_count AS trades_a_to_b
    FROM {{ ref('fact_daily_broker_summary') }}
),

reciprocal_trades AS (
    SELECT
        p1.trade_date,
        p1.symbol,
        p1.broker_a,
        p1.broker_b,
        p1.volume_a_to_b,
        p2.volume_a_to_b AS volume_b_to_a,
        (p1.volume_a_to_b + p2.volume_a_to_b) AS aggregate_matched_volume,
        (p1.amount_a_to_b + p2.amount_a_to_b) AS aggregate_matched_amount,
        (p1.trades_a_to_b + p2.trades_a_to_b) AS total_reciprocal_trades,
        -- Calculate percentage symmetry between reciprocal legs
        LEAST(p1.volume_a_to_b, p2.volume_a_to_b)::NUMERIC / GREATEST(p1.volume_a_to_b, p2.volume_a_to_b) AS volume_symmetry_ratio
    FROM broker_pairs p1
    INNER JOIN broker_pairs p2
        ON  p1.trade_date = p2.trade_date
        AND p1.symbol = p2.symbol
        AND p1.broker_a = p2.broker_b
        AND p1.broker_b = p2.broker_a
    WHERE p1.broker_a < p1.broker_b  -- Avoid duplicate bidirectional reporting
)

SELECT
    trade_date,
    symbol,
    broker_a,
    broker_b,
    aggregate_matched_volume,
    aggregate_matched_amount,
    total_reciprocal_trades,
    ROUND(volume_symmetry_ratio, 4) AS volume_symmetry_ratio,
    CASE 
        WHEN aggregate_matched_amount > 5000000 AND volume_symmetry_ratio > 0.80 THEN 'HIGH_SUSPICION_CIRCULAR_WASH'
        WHEN aggregate_matched_amount > 1000000 AND volume_symmetry_ratio > 0.60 THEN 'MEDIUM_SUSPICION_RECIPROCAL'
        ELSE 'LOW_RISK_MATCH'
    END AS risk_classification
FROM reciprocal_trades
WHERE aggregate_matched_amount >= 500000  -- Minimum NRs. 500k threshold
ORDER BY aggregate_matched_amount DESC

























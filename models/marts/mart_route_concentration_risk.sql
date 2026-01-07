-- mart_route_concentration_risk.sql
-- PURPOSE: Analyze relayer concentration risk per route using CR3 (Share of Top 3 Relayers)
-- METRICS: Filler Dominance Index, Total Volume, Top 3 Volume

{{ config(materialized='table') }}

WITH relayer_volume_by_route AS (
    -- Step 1: Sum volume per relayer per route
    SELECT 
        origin_chain_name,
        destination_chain_name,
        route_name,
        relayer_address,
        SUM(fill_amount_usd) AS relayer_volume_usd
    FROM {{ ref('int_deposit_fill_matching') }}
    WHERE is_filled = TRUE 
      AND relayer_address IS NOT NULL
      AND route_name IS NOT NULL
      AND fill_amount_usd > 0  -- Exclude zero/null amounts
    GROUP BY origin_chain_name, destination_chain_name, route_name, relayer_address
),

ranked_relayers AS (
    -- Step 2: Rank relayers within each route by volume
    SELECT 
        origin_chain_name,
        destination_chain_name,
        route_name,
        relayer_address,
        relayer_volume_usd,
        ROW_NUMBER() OVER (
            PARTITION BY route_name 
            ORDER BY relayer_volume_usd DESC
        ) AS rank_in_route
    FROM relayer_volume_by_route
),

route_totals AS (
    -- Step 3: Calculate total volume per route
    SELECT 
        route_name,
        SUM(relayer_volume_usd) AS total_route_volume_usd
    FROM relayer_volume_by_route
    GROUP BY route_name
    HAVING SUM(relayer_volume_usd) > 0  -- Only routes with positive volume
),

top3_volume AS (
    -- Step 4: Sum volume of top 3 relayers per route
    SELECT 
        origin_chain_name,
        destination_chain_name,
        route_name,
        SUM(relayer_volume_usd) AS top3_volume_usd
    FROM ranked_relayers
    WHERE rank_in_route <= 3
    GROUP BY origin_chain_name, destination_chain_name, route_name
)

-- Step 5: Calculate Filler Dominance Index per Route
SELECT 
    t.origin_chain_name,
    t.destination_chain_name,
    t.route_name,
    ROUND(rt.total_route_volume_usd::NUMERIC, 2) AS total_volume_usd,
    ROUND(t.top3_volume_usd::NUMERIC, 2) AS top_3_relayer_volume_usd,
    ROUND((t.top3_volume_usd / NULLIF(rt.total_route_volume_usd, 0) * 100)::NUMERIC, 2) 
        AS filler_dominance_index_pct,
    -- Risk classification
    CASE 
        WHEN rt.total_route_volume_usd = 0 OR rt.total_route_volume_usd IS NULL THEN 'NO DATA'
        WHEN (t.top3_volume_usd / rt.total_route_volume_usd * 100) > 90 THEN 'CRITICAL'
        WHEN (t.top3_volume_usd / rt.total_route_volume_usd * 100) > 80 THEN 'HIGH'
        WHEN (t.top3_volume_usd / rt.total_route_volume_usd * 100) > 60 THEN 'MODERATE'
        ELSE 'HEALTHY'
    END AS concentration_risk
FROM top3_volume t
JOIN route_totals rt ON t.route_name = rt.route_name
ORDER BY filler_dominance_index_pct DESC

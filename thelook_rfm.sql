-- RFM (Recency, Frequency, Monetary) ver. 1
-- 1. Build the RFM Base Table
WITH rfm_base AS (
  SELECT
    oi.user_id,
    DATE_DIFF(
      DATE '2026-01-01', 
      MAX(DATE(oi.created_at)),
      DAY
    ) + 1                                    AS recency,
    COUNT(DISTINCT oi.order_id)              AS frequency,
    ROUND(SUM(oi.sale_price), 2)             AS monetary,
    ROUND(
      SUM(oi.sale_price) / NULLIF(COUNT(DISTINCT oi.order_id), 0)
    , 2)                                     AS avg_order_value
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  WHERE oi.status NOT IN ('Cancelled', 'Returned')
    AND EXTRACT(year FROM oi.created_at) = 2025
  GROUP BY oi.user_id
)
SELECT *
FROM rfm_base
ORDER BY monetary DESC;

-- 2. Define the RFM Scores
WITH rfm_base AS (
  SELECT
    oi.user_id,
    DATE_DIFF(
      DATE '2026-01-01', 
      MAX(DATE(oi.created_at)),
      DAY
    ) + 1                                    AS recency,
    COUNT(DISTINCT oi.order_id)              AS frequency,
    ROUND(SUM(oi.sale_price), 2)             AS monetary,
    ROUND(
      SUM(oi.sale_price) / NULLIF(COUNT(DISTINCT oi.order_id), 0)
    , 2)                                     AS avg_order_value
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  WHERE oi.status NOT IN ('Cancelled', 'Returned')
    AND EXTRACT(year FROM oi.created_at) = 2025
  GROUP BY oi.user_id
),
rfm_scored AS (
  SELECT
    user_id,
    recency,
    frequency,
    monetary,
    avg_order_value,

    -- Recency: capped at 366 days — use monthly windows
    -- p25=64d, median=143d, p75=244d, p90=314d
    CASE
      WHEN recency <= 30   THEN 5  -- bought in last month
      WHEN recency <= 90   THEN 4  -- bought in last 3 months (top ~20%)
      WHEN recency <= 180  THEN 3  -- bought in last 6 months
      WHEN recency <= 270  THEN 2  -- bought in last 9 months
      ELSE 1                       -- bought 9–12 months ago
    END AS r_score,

    -- Frequency: only 4 values, 84.9% are freq=1
    -- Cannot use NTILE — custom mapping is the only honest option
    CASE
      WHEN frequency = 1  THEN 1   -- one-time buyer   (84.9%)
      WHEN frequency = 2  THEN 3   -- returning buyer  (12.7%)
      WHEN frequency = 3  THEN 4   -- repeat buyer     (2.1%)
      WHEN frequency >= 4 THEN 5   -- loyal buyer      (0.3%)
    END AS f_score,

    -- Monetary: good spread, use your actual percentiles
    -- p25=$34, median=$67.9, p75=$132.4, p90=$227.9
    CASE
      WHEN monetary < 34    THEN 1  -- below p25
      WHEN monetary < 67.9  THEN 2  -- p25 to median
      WHEN monetary < 132.4 THEN 3  -- median to p75
      WHEN monetary < 227.9 THEN 4  -- p75 to p90
      ELSE 5                        -- top 10% spenders
    END AS m_score

  FROM rfm_base
)
SELECT 
  user_id, 
  recency, 
  frequency,
  monetary,
  r_score,
  f_score,
  m_score,
  CONCAT(CAST(r_score AS STRING),
          CAST(f_score AS STRING),
          CAST(m_score AS STRING)) AS rfm_score
FROM rfm_scored
ORDER BY rfm_score DESC;

-- 3. Create Customer Segmentation Labels
WITH rfm_base AS (
  SELECT
    oi.user_id,
    DATE_DIFF(
      DATE '2026-01-01', 
      MAX(DATE(oi.created_at)),
      DAY
    ) + 1                                    AS recency,
    COUNT(DISTINCT oi.order_id)              AS frequency,
    ROUND(SUM(oi.sale_price), 2)             AS monetary,
    ROUND(
      SUM(oi.sale_price) / NULLIF(COUNT(DISTINCT oi.order_id), 0)
    , 2)                                     AS avg_order_value
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  WHERE oi.status NOT IN ('Cancelled', 'Returned')
    AND EXTRACT(year FROM oi.created_at) = 2025
  GROUP BY oi.user_id
),
rfm_scored AS (
  SELECT
    user_id,
    recency,
    frequency,
    monetary,
    avg_order_value,

    -- Recency: capped at 366 days — use monthly windows
    -- p25=64d, median=143d, p75=244d, p90=314d
    CASE
      WHEN recency <= 30   THEN 5  -- bought in last month
      WHEN recency <= 90   THEN 4  -- bought in last 3 months (top ~20%)
      WHEN recency <= 180  THEN 3  -- bought in last 6 months
      WHEN recency <= 270  THEN 2  -- bought in last 9 months
      ELSE 1                       -- bought 9–12 months ago
    END AS r_score,

    -- Frequency: only 4 values, 84.9% are freq=1
    -- Cannot use NTILE — custom mapping is the only honest option
    CASE
      WHEN frequency = 1  THEN 1   -- one-time buyer   (84.9%)
      WHEN frequency = 2  THEN 3   -- returning buyer  (12.7%)
      WHEN frequency = 3  THEN 4   -- repeat buyer     (2.1%)
      WHEN frequency >= 4 THEN 5   -- loyal buyer      (0.3%)
    END AS f_score,

    -- Monetary: good spread, use your actual percentiles
    -- p25=$34, median=$67.9, p75=$132.4, p90=$227.9
    CASE
      WHEN monetary < 34    THEN 1  -- below p25
      WHEN monetary < 67.9  THEN 2  -- p25 to median
      WHEN monetary < 132.4 THEN 3  -- median to p75
      WHEN monetary < 227.9 THEN 4  -- p75 to p90
      ELSE 5                        -- top 10% spenders
    END AS m_score

  FROM rfm_base
),
rfm_labeled AS (
  SELECT
    user_id,
    recency,
    frequency,
    monetary,
    avg_order_value,
    r_score,
    f_score,
    m_score,
    CONCAT(
      CAST(r_score AS STRING),
      CAST(f_score AS STRING),
      CAST(m_score AS STRING)
    ) AS rfm_score,

    CASE
      -- High Recency: customer still active
      WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4                                   THEN 'Champion'
      WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3                                   THEN 'Loyal'
      WHEN r_score >= 3 AND f_score >= 3 AND m_score <= 2                                   THEN 'Potentially Loyal'
      WHEN r_score >= 4 AND f_score = 1 AND m_score <= 2                                    THEN 'New Customer'
      WHEN r_score BETWEEN 3 AND 4 AND f_score <= 2 AND m_score <= 3                        THEN 'Promising'

      -- Mid Recency: engagement start to fade
      WHEN r_score BETWEEN 2 AND 3 AND f_score BETWEEN 2 AND 3 AND m_score BETWEEN 2 AND 3  THEN 'Need Attention'
      WHEN r_score BETWEEN 2 AND 3 AND f_score >= 4 AND m_score >= 4                        THEN 'At Risk'
      WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4                                   THEN 'Cannot Lose'
      
      -- Low Recency: customer has disengaged
      WHEN r_score <= 2 AND f_score BETWEEN 2 AND 3 AND m_score BETWEEN 2 AND 3             THEN 'Hibernating'
      ELSE 'Lost'
    END AS segment

  FROM rfm_scored
)
SELECT *
FROM rfm_labeled
ORDER BY rfm_score DESC;

-- 4. Segment Summary: Count, Revenue, and AVG CLV
WITH rfm_base AS (
  SELECT
    oi.user_id,
    DATE_DIFF(
      DATE '2026-01-01', 
      MAX(DATE(oi.created_at)),
      DAY
    ) + 1                                    AS recency,
    COUNT(DISTINCT oi.order_id)              AS frequency,
    ROUND(SUM(oi.sale_price), 2)             AS monetary,
    ROUND(
      SUM(oi.sale_price) / NULLIF(COUNT(DISTINCT oi.order_id), 0)
    , 2)                                     AS avg_order_value
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  WHERE oi.status NOT IN ('Cancelled', 'Returned')
    AND EXTRACT(year FROM oi.created_at) = 2025
  GROUP BY oi.user_id
),
rfm_scored AS (
  SELECT
    user_id,
    recency,
    frequency,
    monetary,
    avg_order_value,

    -- Recency: capped at 366 days — use monthly windows
    -- p25=64d, median=143d, p75=244d, p90=314d
    CASE
      WHEN recency <= 30   THEN 5  -- bought in last month
      WHEN recency <= 90   THEN 4  -- bought in last 3 months (top ~20%)
      WHEN recency <= 180  THEN 3  -- bought in last 6 months
      WHEN recency <= 270  THEN 2  -- bought in last 9 months
      ELSE 1                       -- bought 9–12 months ago
    END AS r_score,

    -- Frequency: only 4 values, 84.9% are freq=1
    -- Cannot use NTILE — custom mapping is the only honest option
    CASE
      WHEN frequency = 1  THEN 1   -- one-time buyer   (84.9%)
      WHEN frequency = 2  THEN 3   -- returning buyer  (12.7%)
      WHEN frequency = 3  THEN 4   -- repeat buyer     (2.1%)
      WHEN frequency >= 4 THEN 5   -- loyal buyer      (0.3%)
    END AS f_score,

    -- Monetary: good spread, use your actual percentiles
    -- p25=$34, median=$67.9, p75=$132.4, p90=$227.9
    CASE
      WHEN monetary < 34    THEN 1  -- below p25
      WHEN monetary < 67.9  THEN 2  -- p25 to median
      WHEN monetary < 132.4 THEN 3  -- median to p75
      WHEN monetary < 227.9 THEN 4  -- p75 to p90
      ELSE 5                        -- top 10% spenders
    END AS m_score

  FROM rfm_base
),
rfm_labeled AS (
  SELECT
    user_id,
    recency,
    frequency,
    monetary,
    avg_order_value,
    r_score,
    f_score,
    m_score,
    CONCAT(
      CAST(r_score AS STRING),
      CAST(f_score AS STRING),
      CAST(m_score AS STRING)
    ) AS rfm_score,

    CASE
      -- High Recency: customer still active
      WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4                                   THEN 'Champion'
      WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3                                   THEN 'Loyal'
      WHEN r_score >= 3 AND f_score >= 3 AND m_score <= 2                                   THEN 'Potentially Loyal'
      WHEN r_score >= 4 AND f_score = 1 AND m_score <= 2                                    THEN 'New Customer'
      WHEN r_score BETWEEN 3 AND 4 AND f_score <= 2 AND m_score <= 3                        THEN 'Promising'

      -- Mid Recency: engagement start to fade
      WHEN r_score BETWEEN 2 AND 3 AND f_score BETWEEN 2 AND 3 AND m_score BETWEEN 2 AND 3  THEN 'Need Attention'
      WHEN r_score BETWEEN 2 AND 3 AND f_score >= 4 AND m_score >= 4                        THEN 'At Risk'
      WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4                                   THEN 'Cannot Lose'
      
      -- Low Recency: customer has disengaged
      WHEN r_score <= 2 AND f_score BETWEEN 2 AND 3 AND m_score BETWEEN 2 AND 3             THEN 'Hibernating'
      ELSE 'Lost'
    END AS segment

  FROM rfm_scored
)
SELECT
  segment,
  COUNT(user_id)                                                    AS customer_count,
  ROUND(SUM(monetary), 2)                                           AS total_revenue,
  ROUND(AVG(monetary), 2)                                           AS avg_order_value,
  ROUND(AVG(frequency), 2)                                          AS avg_frequency,
  ROUND(AVG(recency), 0)                                            AS avg_recency_days,
  -- CLV proxy: (frequency x monetary) / recency
  ROUND(AVG(SAFE_DIVIDE(frequency * monetary, recency)), 2)         AS avg_clv_proxy
FROM rfm_labeled
GROUP BY segment
ORDER BY total_revenue DESC;


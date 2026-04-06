-- 1. Sell-Through Rate, Dead Stock and Days to Sell by Category
SELECT
  product_category,
  EXTRACT(YEAR FROM created_at) AS year,
  EXTRACT(MONTH FROM created_at) AS month,
  FORMAT_DATE('%B', DATE(created_at)) AS month_name,

  COUNT(id) AS total_stocked,
  COUNTIF(sold_at IS NOT NULL) AS units_sold,
  COUNTIF(sold_at IS NULL) AS dead_stock_units,
  ROUND(SAFE_DIVIDE(COUNTIF(sold_at IS NOT NULL), COUNT(id)) * 100, 2) AS sell_through_rate_pct,
  ROUND(SUM(CASE WHEN sold_at IS NULL THEN cost ELSE 0 END), 2) AS dead_stock_value,

  -- Days to Sell (ONLY for sold items)
  ROUND(AVG(
      CASE 
        WHEN sold_at IS NOT NULL 
        THEN DATE_DIFF(DATE(sold_at), DATE(created_at), DAY)
      END
    ), 1) AS avg_days_to_sell,
  MIN(
    CASE 
      WHEN sold_at IS NOT NULL 
      THEN DATE_DIFF(DATE(sold_at), DATE(created_at), DAY)
    END) AS min_days_to_sell,
  MAX(
    CASE 
      WHEN sold_at IS NOT NULL 
      THEN DATE_DIFF(DATE(sold_at), DATE(created_at), DAY)
    END) AS max_days_to_sell,
  ROUND(STDDEV(
      CASE 
        WHEN sold_at IS NOT NULL 
        THEN DATE_DIFF(DATE(sold_at), DATE(created_at), DAY)
      END), 1) AS stddev_days_to_sell
FROM `bigquery-public-data.thelook_ecommerce.inventory_items`
WHERE EXTRACT(YEAR FROM created_at) = 2025
GROUP BY product_category, year, month, month_name
ORDER BY dead_stock_value DESC;

-- 2. Days On Hand & Turnover by Category, Month
WITH
-- Monthly sales (based on order date)
monthly_sales AS (
  SELECT
    ii.product_category,
    DATE_TRUNC(DATE(oi.created_at), MONTH) AS month_start,
    EXTRACT(YEAR FROM oi.created_at) AS year,
    EXTRACT(MONTH FROM oi.created_at) AS month,
    FORMAT_DATE('%B', DATE(oi.created_at)) AS month_name,

    COUNT(oi.id) AS units_sold,
    SUM(ii.cost) AS cogs   -- COGS proxy

  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` ii
    ON oi.inventory_item_id = ii.id
  WHERE oi.status NOT IN ('Cancelled', 'Returned')
    AND oi.shipped_at IS NOT NULL
    AND EXTRACT(YEAR FROM oi.created_at) = 2025
  GROUP BY ii.product_category, month_start, year, month, month_name
),
-- Monthly inventory (stock received per month)
monthly_inventory AS (
  SELECT
    product_category,
    DATE_TRUNC(DATE(created_at), MONTH) AS month_start,
    SUM(cost) AS inventory_cost_received,
    COUNT(id) AS units_received
  FROM `bigquery-public-data.thelook_ecommerce.inventory_items`
  WHERE EXTRACT(YEAR FROM created_at) = 2025
  GROUP BY product_category, month_start
)
SELECT
  ms.product_category,
  ms.year,
  ms.month,
  ms.month_name,
  ms.units_sold,
  ms.cogs,
  mi.inventory_cost_received,

  -- Inventory Turnover (approx)
  ROUND(SAFE_DIVIDE(ms.cogs, COALESCE(mi.inventory_cost_received, 0)), 3) AS turnover_approx,
  -- Days on Hand (DOH)
  ROUND(SAFE_DIVIDE(30.0, SAFE_DIVIDE(ms.cogs, COALESCE(mi.inventory_cost_received, 0))), 1) AS days_on_hand_approx
FROM monthly_sales ms
LEFT JOIN monthly_inventory mi
  ON ms.product_category = mi.product_category
  AND ms.month_start = mi.month_start
ORDER BY ms.product_category, ms.month;

-- 3. GMROI by Category
-- GMROI = Gross Profit / Inventory Cost (proxy for avg inventory)
SELECT
  dp.product_category,
  ROUND(dp.total_revenue, 2) AS total_revenue,
  ROUND(dp.total_cogs, 2) AS total_cogs,
  ROUND(dp.gross_profit, 2) AS gross_profit,
  ROUND(di.inventory_cost, 2) AS inventory_cost,
  ROUND(SAFE_DIVIDE(dp.gross_profit, di.inventory_cost), 3) AS gmroi,
  ROUND(SAFE_DIVIDE(dp.gross_profit, dp.total_revenue) * 100, 2) AS gross_margin_pct

FROM (
  -- Profit from sold items
  SELECT
    ii.product_category,
    SUM(oi.sale_price) AS total_revenue,
    SUM(ii.cost) AS total_cogs,
    SUM(oi.sale_price - ii.cost) AS gross_profit
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` ii
    ON oi.inventory_item_id = ii.id
  WHERE oi.status NOT IN ('Cancelled', 'Returned') AND EXTRACT(YEAR FROM oi.created_at) = 2025
  GROUP BY ii.product_category
) dp
JOIN (
  -- Calculate average monthly inventory value
  SELECT 
    product_category,
    AVG(monthly_cost) AS inventory_cost -- This provides a truer "Average Investment"
  FROM (
    SELECT 
      product_category,
      EXTRACT(MONTH FROM created_at) as mth,
      SUM(cost) AS monthly_cost
    FROM `bigquery-public-data.thelook_ecommerce.inventory_items`
    WHERE EXTRACT(YEAR FROM created_at) = 2025
    GROUP BY 1, 2
  )
  GROUP BY 1
) 
di
ON dp.product_category = di.product_category
ORDER BY gmroi DESC;
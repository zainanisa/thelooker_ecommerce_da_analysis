-- Look for same cost value between products and inventory_items table
SELECT 
  DISTINCT p.id,
  p.cost,
  i.product_id AS product_id,
  i.cost AS inventory_cost,
  CASE WHEN p.cost = i.cost THEN 'Same Cost Value' ELSE 'Different Cost Value'
  END AS condition
FROM `bigquery-public-data.thelook_ecommerce.products` p
JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` i
  ON p.id = i.product_id;

-- 1. Total Revenue & Gross Profit Margin by Month and Year
-- Profit Margin = (Revenue - COGS) / Revenue
-- Revenue = price * qty product
-- COGS = cost * qty product
SELECT 
  ii.product_category,
  EXTRACT(year FROM oi.created_at) AS year,
  EXTRACT(month FROM oi.created_at) AS month,
  FORMAT_DATE('%B', DATE(oi.created_at)) AS month_name,
  
  COUNT(oi.id) AS total_order,
  ROUND(SUM(oi.sale_price),2) AS revenue,
  ROUND(SUM(ii.cost),2) AS COGS,
  ROUND((SUM(oi.sale_price) - SUM(ii.cost)),2) AS profit,
  ROUND(((SUM(oi.sale_price) - SUM(ii.cost)) / SUM(oi.sale_price) * 100.0),2) AS profit_margin
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` ii
  ON ii.id = oi.inventory_item_id
WHERE oi.status NOT IN ('Cancelled', 'Returned') AND EXTRACT(year FROM oi.created_at) = 2025
GROUP BY ii.product_category, year, month, month_name
ORDER BY year, month, ii.product_category;

-- 2. Return-Adjusted Gross Margin
SELECT
  ii.product_category,
  EXTRACT(year FROM oi.created_at) AS year,
  EXTRACT(month FROM oi.created_at) AS month,
  FORMAT_DATE('%B', DATE(oi.created_at)) AS month_name,

  ROUND(SUM(IF(oi.status != 'Returned', oi.sale_price,        0)), 2) AS delivered_revenue,
  ROUND(SUM(IF(oi.status != 'Returned', ii.cost,              0)), 2) AS delivered_cogs,
  ROUND(SUM(IF(oi.status =  'Returned', oi.sale_price,        0)), 2) AS returned_revenue,
  ROUND(SUM(IF(oi.status =  'Returned', ii.cost,              0)), 2) AS returned_cogs,

  ROUND(SAFE_DIVIDE(
          SUM(IF(oi.status != 'Returned', oi.sale_price - ii.cost, 0)) - SUM(IF(oi.status = 'Returned', ii.cost, 0)),
          SUM(IF(oi.status != 'Returned', oi.sale_price,           0)) - SUM(IF(oi.status = 'Returned', oi.sale_price, 0))
        ) * 100, 2
    ) AS return_adjusted_gross_margin_pct
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` ii
  ON oi.inventory_item_id = ii.id
WHERE oi.status NOT IN ('Cancelled') AND EXTRACT(year FROM oi.created_at) = 2025
GROUP BY ii.product_category, year, month, month_name
ORDER BY year, month, ii.product_category;

-- 3. Price Tier Analysis
SELECT
  ii.product_category,
  EXTRACT(year FROM oi.created_at) AS year,
  EXTRACT(month FROM oi.created_at) AS month,
  FORMAT_DATE('%B', DATE(oi.created_at)) AS month_name,

  CASE
    WHEN oi.sale_price < 20  THEN 'Budget (<$20)'
    WHEN oi.sale_price < 50  THEN 'Mid ($20-$49)'
    WHEN oi.sale_price < 100 THEN 'Premium ($50-$99)'
    ELSE 'Luxury ($100+)'
  END                                 AS price_tier,
  COUNT(oi.id)                        AS units_sold,
  ROUND(SUM(oi.sale_price), 2)        AS total_revenue,
  ROUND(SUM(oi.sale_price - ii.cost), 2)    AS gross_profit,
  ROUND(
    SUM(oi.sale_price - ii.cost)
    / NULLIF(SUM(oi.sale_price), 0) * 100, 2)    AS gross_margin_pct
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` ii
  ON oi.inventory_item_id = ii.id
WHERE oi.status NOT IN ('Cancelled', 'Returned') 
  AND EXTRACT(year FROM oi.created_at) = 2025
GROUP BY price_tier, ii.product_category, year, month, month_name
ORDER BY gross_margin_pct DESC;

-- 4. Markup Price %
SELECT
  ii.product_category,
  EXTRACT(year FROM oi.created_at) AS year,
  EXTRACT(month FROM oi.created_at) AS month,
  FORMAT_DATE('%B', DATE(oi.created_at)) AS month_name,

  COUNT(DISTINCT oi.order_id)       AS order_count,
  COUNT(oi.id)                      AS line_items,
  ROUND(SUM(oi.sale_price), 2)      AS total_revenue,
  ROUND(SUM(ii.cost), 2)            AS total_cogs,
  ROUND(SUM(oi.sale_price - ii.cost), 2)  AS gross_profit,
  ROUND(SUM(oi.sale_price - ii.cost) / NULLIF(SUM(oi.sale_price), 0) * 100, 2)  AS gross_margin_pct,
  ROUND(SUM(oi.sale_price - ii.cost) / NULLIF(SUM(ii.cost), 0) * 100, 2)        AS markup_pct
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` ii
  ON oi.inventory_item_id = ii.id
WHERE oi.status NOT IN ('Cancelled', 'Returned')
GROUP BY ii.product_category, year, month, month_name
ORDER BY gross_profit DESC;

-- 5. Below-cost Sales
SELECT
  ii.product_category,
  EXTRACT(year FROM oi.created_at) AS year,
  EXTRACT(month FROM oi.created_at) AS month,
  FORMAT_DATE('%B', DATE(oi.created_at)) AS month_name,

  COUNT(*)                                AS units_sold_below_cost,
  ROUND(SUM(oi.sale_price - ii.cost), 2)  AS total_loss
FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
JOIN `bigquery-public-data.thelook_ecommerce.inventory_items` ii
  ON oi.inventory_item_id = ii.id
WHERE oi.status NOT IN ('Cancelled', 'Returned')
  AND oi.sale_price < ii.cost 
  AND EXTRACT(year FROM oi.created_at) = 2025
GROUP BY ii.product_category, year, month, month_name
ORDER BY total_loss ASC;


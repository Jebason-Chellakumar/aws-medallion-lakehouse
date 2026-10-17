-- One row per active customer across all tenants.
SELECT
    c.tenant_id,
    c.customer_id,
    c.name,
    c.country,
    COUNT(o.order_id)                AS orders,
    COALESCE(SUM(o.amount), 0)       AS lifetime_value,
    MIN(o.ordered_at)                AS first_order_at,
    MAX(o.ordered_at)                AS last_order_at
FROM silver.customers c
LEFT JOIN silver.orders o
       ON o.tenant_id = c.tenant_id
      AND o.customer_id = c.customer_id
      AND NOT o._is_deleted
      AND o.status NOT IN ('cancelled', 'refunded')
WHERE NOT c._is_deleted
GROUP BY c.tenant_id, c.customer_id, c.name, c.country

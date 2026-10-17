-- Daily revenue per tenant. Legacy tenants have no currency column, so default to USD.
SELECT
    o.tenant_id,
    CAST(o.ordered_at AS DATE)       AS order_date,
    COALESCE(o.currency, 'USD')      AS currency,
    COUNT(*)                         AS orders,
    COUNT(DISTINCT o.customer_id)    AS customers,
    SUM(o.amount)                    AS revenue
FROM silver.orders o
WHERE NOT o._is_deleted
  AND o.status NOT IN ('cancelled', 'refunded')
GROUP BY o.tenant_id, CAST(o.ordered_at AS DATE), COALESCE(o.currency, 'USD')

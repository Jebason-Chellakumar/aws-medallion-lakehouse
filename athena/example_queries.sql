-- Run in the <stack>-analytics Athena workgroup (engine v3 reads Iceberg natively).

-- Revenue by tenant for the last 30 days
SELECT tenant_id, currency, SUM(revenue) AS revenue, SUM(orders) AS orders
FROM gold.tenant_daily_revenue
WHERE order_date >= current_date - INTERVAL '30' DAY
GROUP BY tenant_id, currency
ORDER BY revenue DESC;

-- Top customers across all tenants
SELECT tenant_id, customer_id, name, lifetime_value
FROM gold.customer_lifetime_value
ORDER BY lifetime_value DESC
LIMIT 20;

-- Iceberg time travel: silver as it was yesterday
SELECT COUNT(*) FROM silver.orders
FOR TIMESTAMP AS OF (current_timestamp - INTERVAL '1' DAY);

-- Commit history (one commit per tenant batch per run)
SELECT committed_at, operation, summary['added-records'] AS added
FROM "silver"."orders$snapshots"
ORDER BY committed_at DESC
LIMIT 20;

-- Data freshness per tenant: latest change applied from each source database
SELECT tenant_id, MAX(_dms_ts) AS latest_change
FROM silver.orders
GROUP BY tenant_id
ORDER BY latest_change;

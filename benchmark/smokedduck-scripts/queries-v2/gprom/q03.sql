create temp table lineage as (
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) over (partition by l_orderkey, o_orderdate, o_shippriority) AS revenue,
    o_orderdate,
    o_shippriority,
    customer.rowid as c_rid, 
    orders.rowid as o_rid,
    lineitem.rowid as l_rid
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
);

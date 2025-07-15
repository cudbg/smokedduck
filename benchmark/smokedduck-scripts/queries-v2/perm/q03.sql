create temp table lineage as (
  SELECT Qbase.*, c_rid, o_rid, l_rid
  FROM (
    SELECT customer.rowid as c_rid, orders.rowid as o_rid, lineitem.rowid as l_rid,
        l_orderkey, o_orderdate, o_shippriority
    FROM customer, orders, lineitem
    WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
        AND l_orderkey = o_orderkey AND o_orderdate < CAST('1995-03-15' AS date)
        AND l_shipdate > CAST('1995-03-15' AS date)
  ) as Qplus join (
    SELECT
        l_orderkey, sum(l_extendedprice * (1 - l_discount)) AS revenue, o_orderdate, o_shippriority
    FROM (
      SELECT l_orderkey, l_extendedprice, l_discount, o_orderdate, o_shippriority
      FROM customer, orders, lineitem
      WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
          AND l_orderkey = o_orderkey AND o_orderdate < CAST('1995-03-15' AS date)
          AND l_shipdate > CAST('1995-03-15' AS date)
    )
    GROUP BY l_orderkey, o_orderdate, o_shippriority
    ORDER BY revenue DESC, o_orderdate
    LIMIT 10
  ) as Qbase using (l_orderkey, o_orderdate, o_shippriority)
);

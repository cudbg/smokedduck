create temp table lineage as (
  SELECT n_name,
      sum(l_extendedprice * (1 - l_discount)) over (partition by n_name) AS revenue,
       customer.rowid as c_rid, orders.rowid as o_rid,
       lineitem.rowid as l_rid, supplier.rowid as s_rid,
       nation.rowid as n_rid, region.rowid as r_rid
  FROM customer, orders, lineitem, supplier, nation, region
  WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
     AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
     AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
     AND r_name = 'ASIA' AND o_orderdate >= CAST('1994-01-01' AS date)
     AND o_orderdate < CAST('1995-01-01' AS date)
);

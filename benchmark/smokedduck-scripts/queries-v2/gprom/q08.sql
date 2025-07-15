create temp table lineage as (
SELECT
    o_year,
    sum(
        CASE WHEN nation = 'BRAZIL' THEN
            volume
        ELSE
            0
        END) over (partition by o_year) / sum(volume) over (partition by o_year) AS mkt_share,
       p_rid, c_rid, o_rid,
       l_rid,  s_rid,
       n1_rid,  n2_rid, r_rid
FROM (
    SELECT
        extract(year FROM o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation,
       part.rowid as p_rid,
       customer.rowid as c_rid, orders.rowid as o_rid,
       lineitem.rowid as l_rid, supplier.rowid as s_rid,
       n1.rowid as n1_rid, n2.rowid as n2_rid,
      region.rowid as r_rid
    FROM
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
    WHERE
        p_partkey = l_partkey
        AND s_suppkey = l_suppkey
        AND l_orderkey = o_orderkey
        AND o_custkey = c_custkey
        AND c_nationkey = n1.n_nationkey
        AND n1.n_regionkey = r_regionkey
        AND r_name = 'AMERICA'
        AND s_nationkey = n2.n_nationkey
        AND o_orderdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)
        AND p_type = 'ECONOMY ANODIZED STEEL') AS all_nations
);

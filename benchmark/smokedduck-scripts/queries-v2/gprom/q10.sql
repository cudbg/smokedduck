SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) over (partition by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment)  AS revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment,
       customer.rowid as c_rid, orders.rowid as o_rid,
       lineitem.rowid as l_rid, 
       nation.rowid as n_rid, 
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= CAST('1993-10-01' AS date)
    AND o_orderdate < CAST('1994-01-01' AS date)
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey

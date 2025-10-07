SELECT
    s_name,
    count(*) AS numwait,
    list(supplier.rowid),
    list(l1.rowid),
    list(orders.rowid),
    list(nation.rowid), list(lid)
FROM
    supplier,
    lineitem l1,
    orders,
    nation,
    (SELECT list(l2.rowid) lid, l2.l_orderkey, l2.l_suppkey FROM lineitem l2 GROUP BY l_orderkey, l_suppkey) as t1
WHERE
    s_suppkey = l1.l_suppkey
    AND o_orderkey = l1.l_orderkey
    AND o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND s_nationkey = n_nationkey
    AND n_name = 'SAUDI ARABIA'
    AND t1.l_orderkey = l1.l_orderkey
    AND t1.l_suppkey <> l1.l_suppkey
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100

SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity),
    list(customer.rowid),
    list(orders.rowid),
    list(lineitem.rowid), list(lid)
FROM
    customer,
    orders,
    lineitem,
    (
        SELECT
            l_orderkey, list(lineitem.rowid) as lid
        FROM
            lineitem
        GROUP BY
            l_orderkey
        HAVING
            sum(l_quantity) > 300) as t1
WHERE
    o_orderkey=t1.l_orderkey
    AND c_custkey = o_custkey
    AND o_orderkey = lineitem.l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100

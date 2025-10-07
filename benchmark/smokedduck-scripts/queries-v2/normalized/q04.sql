SELECT
    o_orderpriority,
    count(*) AS order_count,
FROM
    orders,
    (SELECT l_orderkey, 
    FROM lineitem
    WHERE l_commitdate < l_receiptdate
    GROUP BY l_orderkey) as t1
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND t1.l_orderkey = o_orderkey
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority

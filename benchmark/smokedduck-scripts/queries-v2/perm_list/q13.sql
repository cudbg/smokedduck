SELECT
    c_count,
    count(*) AS custdist,
    list(cid), list(oid)
FROM (
    SELECT
        c_custkey,
        count(o_orderkey),
        list(customer.rowid) as cid,
        list(orders.rowid) as oid
    FROM
        customer
    LEFT OUTER JOIN orders ON c_custkey = o_custkey
    AND o_comment NOT LIKE '%special%requests%'
GROUP BY
    c_custkey) AS c_orders (c_custkey,
        c_count)
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC

SELECT qbase.*, o_rid, l_rid
FROM 
    (
    SELECT o_orderpriority, count(*) AS order_count
    FROM orders
    WHERE o_orderdate >= CAST('1993-07-01' AS date) AND o_orderdate < CAST('1993-10-01' AS date)
        AND EXISTS ( SELECT * FROM lineitem  WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate )
    GROUP BY o_orderpriority
  ) as qbase join (
    SELECT o_rid, cb.rowid as l_rid, o_orderpriority FROM
    ( SELECT *, orders.rowid as o_rid FROM orders 
      where o_orderdate >= CAST('1993-07-01' AS date) AND o_orderdate < CAST('1993-10-01' AS date)
        AND EXISTS ( SELECT * FROM lineitem  WHERE l_orderkey = o_orderkey 
                                              AND l_commitdate < l_receiptdate )
    ) as in_plus, (select *, rowid from lineitem
    where l_commitdate < l_receiptdate
  ) AS cb
    WHERE
        EXISTS (
          SELECT *
          FROM (
            select *, rowid from lineitem where l_commitdate < l_receiptdate and l_orderkey = in_plus.o_orderkey
          ) AS Qsub_plus
          WHERE Qsub_plus.rowid=CB.rowid
        )
) AS qplus using (o_orderpriority)

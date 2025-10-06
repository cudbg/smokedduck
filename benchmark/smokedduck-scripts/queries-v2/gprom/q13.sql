select groups2.*,  customer_rowid, orders_rowid
from (
    SELECT customer.rowid as customer_rowid, orders.rowid as orders_rowid,
    c_custkey, count(o_orderkey) over (partition by c_custkey) as c_count
    FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
)  join (
  SELECT c_count, count(*) AS custdist
  FROM (
      SELECT c_custkey, count(o_orderkey) as c_count
      FROM (
        SELECT c_custkey, o_orderkey
        FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
      )
      GROUP BY c_custkey
  )
  GROUP BY c_count
) as groups2 using (c_count)

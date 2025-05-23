select out_index-1 as out_index,  customer, orders
from (
  SELECT customer.rowid as customer, orders.rowid as orders,
         c_custkey, o_orderkey, 
        ROW_NUMBER() OVER (ORDER BY (SELECT o_orderkey)) AS out_index
  FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
)

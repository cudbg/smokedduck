  select groups1.out_index-1 as out_index,  customer, orders
  from (
    SELECT customer.rowid as customer, orders.rowid as orders,
           c_custkey, o_orderkey
    FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
  ) as joins1 left outer join (
    SELECT  ROW_NUMBER() OVER (ORDER BY (SELECT c_custkey)) AS out_index, c_custkey, count(o_orderkey) as c_count
    FROM (
      SELECT c_custkey, o_orderkey
      FROM customer LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
    )
    GROUP BY c_custkey
  ) as groups1 using (c_custkey)

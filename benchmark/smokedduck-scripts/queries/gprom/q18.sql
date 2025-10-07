  select  c_name,  c_custkey,  o_orderkey,  o_orderdate,  o_totalprice,  
  sum_l_quantity,
  c_rid, o_rid, l_rid, in_l.rowid
  from (
        SELECT  c_name,  c_custkey,  o_orderkey,  o_orderdate,  o_totalprice, l_orderkey,
                sum(l_quantity) over (partition by c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice) as sum_l_quantity,
        customer.rowid as c_rid, 
        orders.rowid as o_rid, 
        lineitem.rowid as l_rid 
        FROM customer, orders, lineitem
        WHERE o_orderkey IN (
                SELECT l_orderkey
                FROM lineitem
                GROUP BY l_orderkey
                HAVING sum(l_quantity) > 300)
            AND c_custkey = o_custkey
            AND o_orderkey = l_orderkey
  ) as q, lineitem as in_l 
  WHERE q.o_orderkey=in_l.l_orderkey

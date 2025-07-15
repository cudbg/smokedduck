select distinct l_orderkey, o_orderdate, o_shippriority, revenue from lineage
ORDER BY revenue DESC, o_orderdate;

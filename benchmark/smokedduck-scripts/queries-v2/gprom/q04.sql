create temp table lineage as (
  SELECT select_st.o_orderpriority, select_st.order_count, select_st.o_rid, l_rid
  FROM (
    SELECT o_orderpriority, orders.rowid as o_rid, o_orderkey,
          count(*) over (partition by o_orderpriority) AS order_count
    FROM orders
    WHERE o_orderdate >= CAST('1993-07-01' AS date)
          AND o_orderdate < CAST('1993-10-01' AS date)
          AND EXISTS (SELECT * FROM lineitem
                       WHERE l_commitdate < l_receiptdate
                         and l_orderkey=o_orderkey
                      )
  ) as select_st join (
    SELECT lineitem.rowid as l_rid, l_orderkey
    FROM lineitem
    WHERE l_commitdate < l_receiptdate
  ) as exists_st on ( select_st.o_orderkey=exists_st.l_orderkey)
);

select out_index-1 as out_index, ps_rid as partsupp_2, ps_rid2 as partsupp, s_rid as supplier_2, n_rid as nation, n_rid2 as nation_2, s_rid2 as supplier from (
    select groups.*,
      ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS out_index,
    j1.partsupp_rowid as ps_rid, j1.supplier_rowid as s_rid, j1.nation_rowid as n_rid
    from (
      SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
      FROM (
        SELECT ps_partkey, ps_supplycost, ps_availqty
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
      )
      GROUP BY ps_partkey
    ) as groups join (
      SELECT partsupp.rowid as partsupp_rowid, supplier.rowid as supplier_rowid, nation.rowid as nation_rowid,
             ps_partkey, ps_supplycost, ps_availqty
      FROM partsupp, supplier, nation
      WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
    ) as j1 using (ps_partkey)
  ) as main, (select * from
      (SELECT sum(ps_supplycost * ps_availqty) * 0.000100000 as value_inner
        FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'),
      (SELECT partsupp.rowid as ps_rid2, supplier.rowid as s_rid2, nation.rowid as n_rid2
      FROM partsupp, supplier, nation
      WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY')
  ) as subq
  where value > value_inner
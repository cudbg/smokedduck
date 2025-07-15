create temp table lineage as (
  select main.*, subq.* from (
      SELECT ps_partkey, sum(ps_supplycost * ps_availqty) over (partition by ps_partkey) AS value,
            partsupp.rowid as partsupp_rowid, supplier.rowid as supplier_rowid, nation.rowid as nation_rowid
      FROM partsupp, supplier, nation
      WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'
      ORDER BY value DESC
    ) as main, (select * from
        (SELECT sum(ps_supplycost * ps_availqty) * 0.000100000 as value_inner
          FROM partsupp, supplier, nation WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY'),
        (SELECT partsupp.rowid as ps_rid2, supplier.rowid as s_rid2, nation.rowid as n_rid2
        FROM partsupp, supplier, nation
        WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey AND n_name = 'GERMANY')
    ) as subq
    where value > value_inner
);

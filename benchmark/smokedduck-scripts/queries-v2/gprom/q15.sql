CREATE temp TABLE lineage as (
    select main.*, where_clause.*
    from  ( SELECT supplier.rowid as s_rid, supplier_no, s_suppkey, total_revenue, s_name, s_address, s_phone, l_rid
            FROM supplier, (
              select lineitem.rowid as l_rid, l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) over (partition by l_suppkey) as total_revenue
              from  lineitem
              where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
            ) as revenue0
            WHERE s_suppkey = supplier_no AND total_revenue = (SELECT max(total_revenue) FROM (
              select l_suppkey as supplier_no, sum(l_extendedprice * (1 - l_discount)) as total_revenue
              from  lineitem
              where l_shipdate >= date '1996-01-01' and l_shipdate < date '1996-01-01' + interval '3' month
              group by  l_suppkey
            ) as revenue1)
    )  as main, (
            SELECT lineitem.rowid as l_rid2
              FROM lineitem
              WHERE l_shipdate >= CAST('1996-01-01' AS date)  AND l_shipdate < CAST('1996-04-01' AS date)
    ) as where_clause
);

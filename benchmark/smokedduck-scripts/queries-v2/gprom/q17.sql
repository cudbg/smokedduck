CREATE temp TABLE lineage as (
  select groups.*, lineitem.rowid as lineitem_rowid_2,
  from  (
        SELECT
            sum(l_extendedprice) over () / 7.0 AS avg_yearly,
            lineitem.rowid as lineitem_rowid, part.rowid as part_rowid,
            lineitem.l_partkey
            FROM lineitem, part
            WHERE p_partkey = l_partkey
                AND p_brand = 'Brand#23'
                AND p_container = 'MED BOX'
                AND l_quantity < (
                    SELECT
                        0.2 * avg(l_quantity)
                    FROM
                        lineitem
                    WHERE
                        l_partkey = p_partkey)
    ) as groups, lineitem
  where lineitem.l_partkey=groups.l_partkey
);

with lineage as (
  select l_returnflag, l_linestatus, lineitem_rowid
  from (
    SELECT
        l_returnflag, l_linestatus, 
    FROM (
      SELECT
          lineitem.rowid as lineitem_rowid,
          l_returnflag,l_linestatus
      FROM lineitem
      WHERE l_shipdate <= CAST('1998-09-02' AS date)
    )
    GROUP BY l_returnflag, l_linestatus
  ) as groups join (
    SELECT
        lineitem.rowid as lineitem_rowid,l_returnflag, l_linestatus
    FROM lineitem
    WHERE l_shipdate <= CAST('1998-09-02' AS date)
  ) using (l_returnflag, l_linestatus)
)

select count(*) as c, max(lineitem_rowid)  from lineage

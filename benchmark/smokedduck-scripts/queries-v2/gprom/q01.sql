create temp table lineage as (
  SELECT
      rowid,
      l_returnflag,
      l_linestatus,
      sum(l_quantity) over (partition by l_returnflag, l_linestatus) AS sum_qty,
      sum(l_extendedprice) over (partition by l_returnflag, l_linestatus) AS sum_base_price,
      sum(l_extendedprice * (1 - l_discount)) over (partition by l_returnflag, l_linestatus) AS sum_disc_price,
      sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) over (partition by l_returnflag, l_linestatus)  AS sum_charge,
      avg(l_quantity) over (partition by l_returnflag, l_linestatus) AS avg_qty,
      avg(l_extendedprice) over (partition by l_returnflag, l_linestatus) AS avg_price,
      avg(l_discount) over (partition by l_returnflag, l_linestatus) AS avg_disc,
      count(*) over (partition by l_returnflag, l_linestatus) AS count_order
  FROM
      lineitem
  WHERE
      l_shipdate <= CAST('1998-09-02' AS date)
);

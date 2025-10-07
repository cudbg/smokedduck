SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment,
    part.rowid,
    supplier.rowid,
    partsupp.rowid,
    nation.rowid,
    region.rowid,
FROM
    part,
    supplier,
    partsupp,
    nation,
    region,
    (
        SELECT
            ps_partkey,
            min(ps_supplycost) v,
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE'
            group by ps_partkey
          ) t1
WHERE
    p_partkey = partsupp.ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = t1.v AND t1.ps_partkey=p_partkey
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100

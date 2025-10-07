SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly,
FROM
    lineitem,
    part,
    (
        SELECT
            0.2 * avg(l_quantity) v, l_partkey
        FROM
            lineitem group by l_partkey) t1
WHERE
    p_partkey = lineitem.l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND t1.l_partkey = p_partkey
    AND l_quantity < t1.v

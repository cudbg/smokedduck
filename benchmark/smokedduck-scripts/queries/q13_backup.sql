SELECT
    c_custkey,
    o_orderkey
FROM
    customer
LEFT OUTER JOIN orders ON c_custkey = o_custkey
AND o_comment NOT LIKE '%special%requests%'
order by o_orderkey

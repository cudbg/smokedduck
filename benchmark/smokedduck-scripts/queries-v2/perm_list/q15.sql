with revenue0 (supplier_no, total_revenue, lid) as (
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount)),
    list(lineitem.rowid) as lid
	from
		lineitem
	where
		l_shipdate >= date '1996-01-01'
		and l_shipdate < date '1996-01-01' + interval '3' month
	group by
		l_suppkey
)

SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue,
    supplier.rowid, lid, t1.r0_lid
FROM
    supplier, revenue0, (
        SELECT
            max(total_revenue) as v, list(lid) as r0_lid
        FROM revenue0) as t1
WHERE
    s_suppkey = supplier_no
    AND total_revenue = t1.v
ORDER BY
    s_suppkey

SELECT distinct
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from lineage
ORDER BY
    s_suppkey;

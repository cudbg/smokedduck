SELECT distinct p_brand, p_type, p_size, supplier_cnt
from lineage
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size;

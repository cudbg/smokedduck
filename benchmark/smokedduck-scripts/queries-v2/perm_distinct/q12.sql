SELECT distinct l_shipmode,
        high_line_count,
        low_line_count
from lineage
ORDER BY l_shipmode;

select distinct l_returnflag, l_linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order
from lineage
ORDER BY
    l_returnflag,
    l_linestatus;

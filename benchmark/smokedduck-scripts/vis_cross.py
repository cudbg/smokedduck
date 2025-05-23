from timeit import default_timer as timer
import duckdb
import os
from pygg import *
import pandas as pd
from utils import legend_bottom, legend_side, legend_none

sf = 1
dbname = f'tpch_{sf}.db'
if not os.path.exists(dbname):
    con = duckdb.connect(dbname)
    con.execute("CALL dbgen(sf="+str(sf)+");")
else:
    con = duckdb.connect(dbname)

print(con.execute("pragma table_info('lineitem')").df());
print(con.execute("pragma table_info('part')").df());
print(con.execute("pragma table_info('orders')").df());
#3where l_partkey=91705
#where monthname(o.o_orderdate) = 'November'
q1 = """
SELECT r.r_name AS region, SUM(l.l_extendedprice) AS total_sales
FROM region r
JOIN nation n ON r.r_regionkey = n.n_regionkey JOIN customer c ON n.n_nationkey = c.c_nationkey
JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON o.o_orderkey = l.l_orderkey
GROUP BY r.r_name;
"""
start = timer()
df1 = con.execute(q1).df()
end = timer()
print(end - start)
print(df1)

q2 = """
SELECT monthname(o.o_orderdate) AS month, SUM(l.l_extendedprice) AS total_sales
FROM region r
JOIN nation n ON r.r_regionkey = n.n_regionkey JOIN customer c ON n.n_nationkey = c.c_nationkey
JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON o.o_orderkey = l.l_orderkey
where r_name='ASIA'
GROUP BY month
ORDER BY month;
"""
start = timer()
df2 = con.execute(q2).df()
end = timer()
print(end - start)
print(df2)

q3 = """
SELECT
    p.p_name AS part_name,
    p.p_partkey AS part_id,
    SUM(l.l_quantity) AS total_quantity
FROM
    lineitem l
JOIN
    part p ON l.l_partkey = p.p_partkey
GROUP BY
    p.p_name, p.p_partkey
"""
start = timer()
df3 = con.execute(q3).df()
end = timer()
print(end - start)
print(df3)

q4 = """
SELECT dayname(o.o_orderdate) AS day, SUM(l.l_extendedprice) AS total_sales
FROM region r
JOIN nation n ON r.r_regionkey = n.n_regionkey JOIN customer c ON n.n_nationkey = c.c_nationkey
JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON o.o_orderkey = l.l_orderkey
where monthname(o.o_orderdate)='September'
GROUP BY day
ORDER BY day;
"""
start = timer()
df4 = con.execute(q4).df()
end = timer()
print(end - start)
print(df4)


out1 = con.execute("select region, total_sales / 1000000000  as total_sales from df1").df()
print(out1)
#out1 = df1
p = ggplot(out1, aes(x='region', y='total_sales', fill='region'))
p += geom_bar(stat=esc('identity'), alpha=0.8, posiion=position_dodge(width=0.1), width=0.8)
p += axis_labels('Region', "Total Sales * 1B", "discrete")
p += legend_none
ggsave("figures/viz1.png", p,  width=8, height=4, scale=0.8)

out2 = con.execute("select month, total_sales / 1000000000  as total_sales from df2").df()
print(out2)
p = ggplot(out2, aes(x='month', y='total_sales', fill='month', color='month'))
p += geom_bar(stat=esc('identity'), alpha=0.8, posiion=position_dodge(width=0.1), width=0.8)
xkwargs=dict(labels=list(map(esc, ['Jan', 'Feb', 'Mar', 'April', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])))
morder = ["'January'", "'Feburary'", "'March'", "'April'", "'May'", "'June'", "'July'", "'August'", "'September'", "'October'", "'November'", "'December'"]
morder = ','.join(morder)
postfix = """data$month= factor(data$month, levels=c({}))""".format(morder)
p += axis_labels('Month', "Total Sales * 1B", "discrete", xkwargs=xkwargs)
p += legend_none
ggsave("figures/viz2.png", p,  postfix=postfix, width=8, height=4, scale=0.8)

p = ggplot(df4, aes(x='day', y='total_sales', fill='day', color='day'))
p += geom_bar(stat=esc('identity'), alpha=0.8, posiion=position_dodge(width=0.1), width=0.8)
p += axis_labels('Month', "Total Sales * 1B", "discrete")
p += legend_none
ggsave("figures/viz3.png", p,   width=8, height=4, scale=0.8)

print(con.execute("select count(distinct p_type), count(distinct p_size) from part").df())

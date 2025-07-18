import duckdb

con = duckdb.connect('micro_benchmark_exp_agg.out')
print(con.execute("pragma show_tables").df())
agg_df = con.execute("select * from agg_results").df()
print(agg_df)

con = duckdb.connect('micro_benchmark_exp_filter.out')
print(con.execute("pragma show_tables").df())
filter_df = con.execute("select * from filter_results").df()
print(filter_df)

con = duckdb.connect('micro_benchmark_exp_hj.out')
print(con.execute("pragma show_tables").df())
join_df = con.execute("select * from join_results").df()
print(join_df)

con = duckdb.connect('micro_benchmark_exp_ineq.out')
ineq_df = con.execute("select * from join_results").df()
print(ineq_df)

con = duckdb.connect("micro_benchmark_exp_20250717_0230.out")
perm_agg= con.execute("select * from agg_results").df()
perm_filter = con.execute("select * from filter_results").df()
perm_join = con.execute("select * from join_results").df()

con = duckdb.connect('micro_benchmark_all.db')
con.execute("create table agg_results as select * from agg_df UNION ALL select * from perm_agg")
con.execute("create table filter_results as select * from filter_df UNION ALL select * from perm_filter")
con.execute("create table join_results as select * from join_df UNION ALL select * from ineq_df UNION ALL select * from perm_join")

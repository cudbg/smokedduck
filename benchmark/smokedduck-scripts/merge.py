import duckdb
db1 = 'tpch_benchmark_capture_exp_20241002_2244.db'
con = duckdb.connect(db1)
df1 = con.execute("select * from tpch_capture").df()
print(df1)

db2 = 'tpch_benchmark_capture_exp_20241002_2321.db'
con = duckdb.connect(db2)
df2 = con.execute("select * from tpch_capture").df()
print(df2)
df3 = con.execute("select * from df1 UNION ALL select * from df2").df()
print(df3)

db3 = 'tpch_benchmark_capture_exp_20241002_2054.db'
con = duckdb.connect(db3)
df4 = con.execute("select * from tpch_capture").df()
df5 = con.execute("select * from df3 UNION ALL select * from df4").df()
print(df5)

con = duckdb.connect('tpch_benchmark_capture_exp.db')
con.execute("create table tpch_capture as select * from df5")

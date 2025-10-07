import duckdb

con = duckdb.connect('tpch_bw_logical_all.db')
print(con.execute("pragma show_tables").df())
df1 = con.execute("select * from tpch_bw").df()
print(df1)

con = duckdb.connect('tpch_bw_lineage_seen.db')
print(con.execute("pragma show_tables").df())
df2 = con.execute("select * from tpch_bw").df()
print(df2)

con = duckdb.connect('tpch_bw_seen.db')
con.execute("create table tpch_bw as select * from df1 UNION ALL select * from df2")

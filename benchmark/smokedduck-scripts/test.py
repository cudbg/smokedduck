# Q2 (opid 16, 27),  Q4, Q17,  Q20, Q21, Q22 (opid 19)
import datetime
from timeit import default_timer as timer
import json
import duckdb
import pandas as pd
import argparse
import csv
import os

def get_predicates(df, i, qkeys):
    print("get predicate", qkeys)
    predicate=''
    for k in qkeys:
        if len(k) == 0:
            continue
        if len(predicate)>0:
            predicate+=" AND "
        val = df.loc[i, k]
        if isinstance(val, int) or isinstance(val, float):
            predicate+=k+"="+str(val)
        elif isinstance(val, str):
            predicate+=k+ "='"+str(val)+"'"
        elif isinstance(val,datetime.date):
            predicate+=k+"='"+str(val)+"'"
        else:
            predicate+=k+"="+str(val)
    # Add the where predicate to the lineage query
    if len(predicate) > 0:
        predicate = " where " + predicate
    return predicate



pd.set_option('display.max_colwidth', None)  # Show full string content
pd.set_option('display.max_columns', None)   # Show all columns
pd.set_option('display.width', 0)            # Avoid line-wrapping

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--folder', type=str, help='queries folder', default='benchmark/smokedduck-scripts/')
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--qid', type=int, help="query id", default=1)
parser.add_argument('--opid', type=int, help="operator id", default=1)
parser.add_argument('--oid', type=int, help="operator id", default=0)
args = parser.parse_args()

prefix = args.folder + "queries-v2/q"
dbname = f'tpch_{args.sf}.db'
if not os.path.exists(dbname):
    con = duckdb.connect(dbname)
    con.execute("CALL dbgen(sf="+str(args.sf)+");")
else:
    con = duckdb.connect(dbname)

print(con.execute("""
SELECT *
FROM duckdb_functions()
WHERE function_type = 'table' AND function_name LIKE '%lineage%'
""").df())

con.execute("PRAGMA threads=1")
qfile = prefix+str(args.qid).zfill(2)+".sql"
text_file = open(qfile, "r")
query = text_file.read().strip()
query = ' '.join(query.split())
print(query)
con.execute("PRAGMA enable_lineage")
df = con.execute(query).fetchdf()
con.execute("PRAGMA disable_lineage")
print(df)

q_list = con.execute("select * from duckdb_queries_list()").df()
print(con.execute("select build_time, postprocess_time, plan from q_list").df())

# TODO: q16 op21 -> gb probe (need to deal with distinct)

start = timer()
out = con.execute(f"select * from lineage_query(1, {args.opid}, {args.oid}::UINTEGER)")
end = timer()
print(end - start)
print(out.df())
#print(con.execute(f"select * from lineage_view(1, {args.opid})").df())
con.execute("PRAGMA clear_lineage")
    
perm_prefix ="benchmark/smokedduck-scripts/queries-v2/perm_bw/q"
qkeys = "benchmark/smokedduck-scripts/queries-v2//perm_keys/q"+str(args.qid).zfill(2)+".sql"
qkeys_text_file = open(qkeys, "r")
qkeys = qkeys_text_file.read()
qkeys = " ".join(qkeys.split())
qkeys = qkeys.split(',')
predicate = get_predicates(df, args.oid, qkeys)
print(predicate)


q = perm_prefix+str(args.qid).zfill(2)+".sql"
text_file = open(q, "r")
tpch = text_file.read()
tpch = " ".join(tpch.split()) + " " + predicate
text_file.close()
print(tpch)
            
df_bw = con.execute(tpch).fetchdf()
print(df_bw)

import duckdb
import pandas as pd
import argparse
import csv
import os

from utils import parse_plan_timings, Run, DropLineageTables, getStats


parser = argparse.ArgumentParser(description='TPCH benchmarking script')

parser.add_argument('--notes', type=str,  help="run notes")
parser.add_argument('--lineage', action='store_true',  help="use smokedduck")
parser.add_argument('--perm', action='store_true',  help="use perm queries")
parser.add_argument('--smoke', action='store_true',  help="use smoke")
parser.add_argument('--hg', action='store_true',  help="use hash group by")
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
parser.add_argument('--folder', type=str, help='queries folder', default='benchmark/smokedduck-scripts/')
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--save', action='store_true',  help="save result in csv")
parser.add_argument('--show_tables', action='store_true',  help="List tables")
parser.add_argument('--show_output', action='store_true',  help="Print query output")

args = parser.parse_args()
args.profile = True
args.stats = True

size_avg = 0.0

lineage_type = "Baseline"
prefix = args.folder + "queries/q"
table_name=None
if args.perm:
    prefix = args.folder + "queries/perm/q"
    args.lineage_query = False
    lineage_type = "Perm"
    table_name='lineage'
elif args.lineage:
    lineage_type = "SD"
elif args.smoke:
    lineage_type = "Smoke"

qid = 1
args.qid = qid
dbname = f'tpch_{args.sf}.db'
if not os.path.exists(dbname):
    con = duckdb.connect(dbname)
    con.execute("CALL dbgen(sf="+str(args.sf)+");")
else:
    con = duckdb.connect(dbname)
    con.execute("PRAGMA threads=1")

qfile = prefix+str(qid).zfill(2)+".sql"
text_file = open(qfile, "r")
query = text_file.read().strip()
query = ' '.join(query.split())
print(query)
text_file.close()
print("%%%%%%%%%%%%%%%% Running Query # 1")
if args.smoke and args.hg:
    con.execute(f"PRAGMA set_agg('reg')")

avg, df = Run(query, args, con, table_name)
print(df)
plan_timings = {}
plan_full = {}
if args.profile:
    plan_timings, plan_full = parse_plan_timings(args.qid)
output_size = len(df)
if table_name:
    df = con.execute("select count(*) as c from {}".format(table_name)).fetchdf()
    output_size = df.loc[0,'c']
    con.execute("DROP TABLE "+table_name)
print("**** output size: ", output_size)
if args.lineage:
    DropLineageTables(con)
    
hash_agg_force = False
if args.smoke and args.hg:
    con.execute("PRAGMA set_agg('clear')")
    hash_agg_force = True

results = [{'query': qid, 'runtime': avg, 'sf': args.sf, 'repeat': args.repeat,
    'lineage_type': lineage_type, 'output': output_size, 'hash_agg_force': hash_agg_force,
    'notes': args.notes, 'plan_timings': str(plan_timings), 'plan': str(plan_full)}]

if args.save:
    dbname="baselines_{}.db".format(args.notes)
    print(dbname)
    data = pd.DataFrame(results)
    if not os.path.exists(dbname):
        con = duckdb.connect(dbname)
        con.execute(f"""create or replace table baselines as select * from data""")
        print(con.execute("select * from baselines").df())
    else:
        con = duckdb.connect(dbname)
        con.execute(f"""create table if not exists baselines as select * from data where 1=0""")
        con.execute(f"""INSERT INTO baselines SELECT * from data""")
        print(con.execute("select * from baselines").df())

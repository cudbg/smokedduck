# python3 tpch_capture.py feb11  --repeat 3  --save_csv  --perm --csv_append
# python3 tpch_capture.py feb11  --repeat 3  --save_csv  --csv_append
import json
import duckdb
import pandas as pd
import argparse
import csv
import os

from utils import parse_plan_timings, Run, getStats

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--mat', action='store_true',  help="store in lineage table")
parser.add_argument('--normalized', action='store_true',  help="uncorrelate")
parser.add_argument('--lineage', action='store_true',  help="Enable lineage")
parser.add_argument('--show_tables', action='store_true',  help="List tables")
parser.add_argument('--show_output', action='store_true',  help="Print query output")
parser.add_argument('--stats', action='store_true',  help="Get lineage size, nchunks and postprocess time")
parser.add_argument('--query_lineage', action='store_true',  help="query lineage")
parser.add_argument('--perm', action='store_true',  help="use perm queries")
parser.add_argument('--smoke', action='store_true',  help="use smoke queries")
parser.add_argument('--gprom', action='store_true',  help="use perm queries")
parser.add_argument('--logical_list', action='store_true',  help="use list(rowid)")
parser.add_argument('--opt', action='store_true',  help="use optimized")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
parser.add_argument('--folder', type=str, help='queries folder', default='benchmark/smokedduck-scripts/')
args = parser.parse_args()
args.profile = True
args.stats = True
print(args.profile)

prefix = args.folder + "queries-v2/q"
table_name=None
if args.perm:
    prefix = args.folder + "queries-v2/perm/q"
    args.lineage_query = False
    lineage_type = "Logical-RID"
    if args.opt:
        prefix = args.folder + "queries-v2/optimized_perm/q"
        lineage_type = "Logical-OPT"
elif args.gprom:
    prefix = args.folder + "queries-v2/gprom/q"
    args.lineage_query = False
    lineage_type = "Logical-window"
elif args.logical_list:
    prefix = args.folder + "queries-v2/perm_list/q"
    args.lineage_query = False
    lineage_type = "Logical-list"
elif args.lineage:
    lineage_type = "SD_Capture"
else:
    lineage_type = "Baseline"
if args.normalized:
    prefix = args.folder + "queries-v2/normalized/q"
    lineage_type += "_normalized"
# sf: 1, 5, 10, 20
# threads: 1, 4, 8, 12, 16
threads_list = [1, 4, 8, 16]
opt_queries = [2, 4, 15, 16, 17, 20, 21]
dont_scale = [2, 4, 17, 20, 21] #, 4, 16, 17, 20, 21, 22]
dont_scale_10 = [11, 22]
dont_scale_20 = [11, 16, 22]
gprom_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22]

# 4, 16, 18, 20, 21, 22 join is empty. check why.
# 11 nested loop join

# 3, 7, 4
results = []
sf = args.sf
# 4
# semi join:, 20
for th_id in threads_list:
    for i in range(1, 23):
        dbname = f'tpch_{sf}.db'
        if not os.path.exists(dbname):
            con = duckdb.connect(dbname)
            con.execute("CALL dbgen(sf="+str(sf)+");")
        else:
            con = duckdb.connect(dbname)
        con.execute("PRAGMA threads="+str(th_id))
        
        print(f"running {th_id}, {i}")
        if args.gprom and i not in gprom_list: continue
        if (args.perm and args.opt == False) and ((i in dont_scale) or (sf>10 and i in dont_scale_20) or (sf==10 and i in dont_scale_10)): continue
        if (args.gprom) and  ((sf>10 and i in dont_scale_20) or (sf==10 and i in dont_scale_10)): continue
        if args.perm and args.opt and i not in opt_queries: continue
        if args.logical_list and i == 22: continue
        args.qid = i
        # TODO: if i == 11 then replace the constant
        qfile = prefix+str(i).zfill(2)+".sql"
        text_file = open(qfile, "r")
        query = text_file.read().strip()
        query = ' '.join(query.split())
        if sf == 10 and i == 11:
            query = query.replace('0.000100000', '0.0000100000')
        print(query)
        text_file.close()
        print("%%%%%%%%%%%%%%%% Running Query # ", i, " threads: ", th_id)
        if args.mat:
            table_name='lineage'
            query = f"create temp table {table_name} as ( {query} );"

        avg, df = Run(query, args, con, table_name)
        
        plan_timings = {}
        plan_full = {}
        if args.profile:
            plan_timings, plan_full = parse_plan_timings(args.qid)
        table_size, lineage_size, lineage_count, nchunks, postprocess_time = 0, 0, 0, 0, 0
        plan = None
        if args.lineage and args.stats and th_id == 1:
            lineage_size, lineage_count, nchunks, postprocess_time, build_time, plan = getStats(con, query)
            print(plan)
            postprocess_time *= 1000
        
        if args.show_tables:
            tables = con.execute("PRAGMA show_tables").fetchdf()
            print(tables)
        if args.lineage:
            con.execute("PRAGMA clear_lineage")
        
        if args.mat:
            df = con.execute(f"select count(*) as c from {table_name}").fetchdf()
            output_size = df.loc[0,'c']
            con.execute(f"copy {table_name} to '{table_name}.parquet' (format parquet, compression 'uncompressed');")
            table_size = round(os.path.getsize(f"{table_name}.parquet") / (1024.0 * 1024.0))
            os.remove(f"{table_name}.parquet")
            con.execute("drop table "+table_name)
        else:
            output_size = len(df)
        
        # todo: get size of db prio and after, subtract -> perm size
        if args.perm or args.gprom:
            lineage_size = table_size
        print("**** output size: ", output_size, ", table_size: " , table_size)
        
        results.append({'query': i, 'runtime': avg, 'sf': sf, 'repeat': args.repeat,
            'lineage_type': lineage_type, 'n_threads': th_id, 'output': output_size,
            'lineage_size': lineage_size, 'lineage_count': lineage_count, 'table_size': table_size,
            'nchunks': nchunks, 'postprocess_time': postprocess_time,
            'notes': args.notes, 'plan_timings': str(plan_timings), 'plan': str(plan_full)})
os.remove(dbname)
if args.save_csv:
    dbname="tpch_benchmark_capture_{}.db".format(args.notes)
    print(dbname)
    con = duckdb.connect(dbname)
    data = pd.DataFrame(results)
    if args.csv_append:
        con.execute(f"""create table if not exists tpch_capture as select * from data where 1=0""")
        con.execute(f"""INSERT INTO tpch_capture SELECT * from data""")
        print(con.execute("select * from tpch_capture").df())
    else:
        con.execute(f"""create or replace table tpch_capture as select * from data""")
        print(con.execute("select * from tpch_capture").df())

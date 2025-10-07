### Benchmark Perm backward queries by rerunning the original query with predicate
### for Q11, use for queries/q11.sql and perm_bw/q11.sql
### sf10: sum(ps_supplycost * ps_availqty) * 0.0000100000
### sf1: sum(ps_supplycost * ps_availqty) * 0.0001000000
from timeit import default_timer as timer
import duckdb
import datetime
import pandas as pd
import argparse
import csv
import random
import sys
import os
# Add parent directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils import parse_plan_timings, Run, getStats

parser = argparse.ArgumentParser(description='TPCH single script')
# results management
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
parser.add_argument('--show_tables', action='store_true',  help="list tables")
parser.add_argument('--show_output', action='store_true',  help="query output")
# lineage system
parser.add_argument('--lineage', action='store_true',  help="Enable lineage")
parser.add_argument('--hybrid', action='store_true',  help="Enable Hybrid")
parser.add_argument('--op_level', action='store_true',  help="Enable Op-level")

parser.add_argument('--perm', action='store_true',  help="use perm queries")
parser.add_argument('--gprom', action='store_true',  help="use perm queries")
parser.add_argument('--logical_list', action='store_true',  help="use list(rowid)")
parser.add_argument('--opt', action='store_true',  help="use optimized")
parser.add_argument('--normalized', action='store_true',  help="uncorrelate")

# benchmark setting
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
parser.add_argument('--folder', type=str, help='queries folder', default='benchmark/smokedduck-scripts/')
args = parser.parse_args()
args.no_persist = False
args.smoke = False
args.profile = True
args.stats = True
print("script args: ", args)

results = []
base = args.folder + "/queries/"

# generate TPCH workload
sf = args.sf
dbname = f'tpch_{sf}.db'
if not os.path.exists(dbname):
    con = duckdb.connect(dbname)
    con.execute("CALL dbgen(sf="+str(sf)+");")
else:
    con = duckdb.connect(dbname)

con.execute("PRAGMA threads=1")

def get_predicates(df, test_out, qkeys):
    predicates = []
    for i in test_out:
        # construct the predicate for the perm query
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
        predicates.append(predicate)
    return predicates


for qid in range(1, 23):
    if qid in [16]: continue
    print("=======" + str(qid) + "========")
    args.qid = qid
    
    # collect query attributes to reference when running
    # BW queries by applying predicates
    qkeys = base+"/perm_keys/q"+str(qid).zfill(2)+".sql"
    qkeys_text_file = open(qkeys, "r")
    qkeys = qkeys_text_file.read()
    qkeys = " ".join(qkeys.split())
    qkeys = qkeys.split(',')
    
    # read base query = tpch query qid
    q = base+"/q"+str(qid).zfill(2)+".sql"
    text_file = open(q, "r")
    base_q = text_file.read()
    base_q = " ".join(base_q.split())
    text_file.close()

    if sf == 10 and qid == 11:
        base_q = base_q.replace('0.000100000', '0.0000100000')
    # run base query

    lineage = args.lineage
    args.lineage = False
    base_avg, df = Run(base_q, args, con)
    base_plan_timings = {}
    base_plan_full = {}
    if args.profile:
        base_plan_timings, base_plan_full = parse_plan_timings(args.qid)
    args.lineage = lineage

    perm_prefix = base + "/perm_bw/q"
    max_ntuples = 10
    # cap number of bw queries to max_ntuples
    sample_size = min(len(df), max_ntuples)
    print("Running ", sample_size, "queries for qid:", qid)
    # pick random tuples ID to run backward queries on
    test_out = random.sample(range(0, len(df)), sample_size)
    print("Tuples ID for BW: ", test_out)
    if sample_size == 0:
        continue
    if args.lineage:
        con.execute("PRAGMA clear_lineage")
        
        con.execute("PRAGMA enable_lineage")
        df = con.execute(base_q).fetchdf()
        con.execute("PRAGMA disable_lineage")
        
        lineage_size, lineage_count, nchunks, postprocess_time, build_time = 0, 0, 0, 0, 0
        query_info = con.execute("select * from duckdb_queries_list()").df()
        nl = len(query_info)-1
        query_id = query_info.loc[nl, 'query_id']
        lineage_size = query_info.loc[nl, 'size_mb']
        lineage_count = query_info.loc[nl, 'tuples_count']
        nchunks = query_info.loc[nl, 'nchunks']
        postprocess_time = query_info.loc[nl, 'postprocess_time']
        build_time = query_info.loc[nl, 'build_time']
        print("---> qid", query_id, "build time: ", build_time)
        
        for oid in test_out:
            start = timer()
            df_out = con.execute(f"""select max(temp.iid), count() c from (select *  from lineage_query({query_id}, 100, {oid}::UINTEGER) as t(t, oid, iid)) as temp""").df()
            end = timer()
            output_size = df_out.loc[0, 'c']
            print(end - start, output_size)
            results.append({'query': qid, 'runtime': end-start, 'sf': args.sf,
                'build_time': build_time, 'post_process': postprocess_time, 'lineage_size': lineage_size, 'lineage_count': lineage_count, 'nchunks': nchunks,
                'repeat': args.repeat, 'lineage_type': "SD", 
                'output_size': output_size, 'base_size': len(df), 'plan': '',
                'plan_timings': '',
                'base_runtime': base_avg, 'base_plan': str(base_plan_full), 'base_timings': str(base_plan_timings)})
        con.execute("PRAGMA clear_lineage")

    else:
        predicates = get_predicates(df, test_out, qkeys)
        for predicate in predicates:
            # read perm query
            q = perm_prefix+str(qid).zfill(2)+".sql"
            text_file = open(q, "r")
            tpch = text_file.read()
            tpch = " ".join(tpch.split()) + " " + predicate
            text_file.close()
            
            print("%%%%%%%%%%%%%%%% Running Query # ", qid)
            avg, df_out = Run(tpch, args, con)
            plan_timings = {}
            plan_full = {}
            if args.profile:
                plan_timings, plan_full = parse_plan_timings(args.qid)
            # return how many tuples returned by the BW query
            output_size = df_out.loc[0, 'c']
            print("avg: ", avg, "base avg: ", base_avg)
            results.append({'query': qid, 'runtime': avg, 'sf': args.sf, 
                'build_time': 0, 'post_process': 0, 'lineage_size': 0, 'lineage_count': 0, 'nchunks': 0,
                'repeat': args.repeat, 'lineage_type': "Logical-RID", 
                'output_size': output_size, 'base_size': len(df), 'plan': str(plan_full),
                'plan_timings': str(plan_timings),
                'base_runtime': base_avg, 'base_plan': str(base_plan_full), 'base_timings': str(base_plan_timings)})

if args.save_csv:
    filename="tpch_bw_"+args.notes+".db"
    con = duckdb.connect(filename)
    data = pd.DataFrame(results)
    print(filename)
    if args.csv_append:
        con.execute(f"""create table if not exists tpch_bw as select * from data where 1=0""")
        con.execute(f"""INSERT INTO tpch_bw SELECT * from data""")
        print(con.execute("select * from tpch_bw").df())
    else:
        con.execute(f"""create or replace table tpch_bw as select * from data""")
        print(con.execute("select * from tpch_bw").df())


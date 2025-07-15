from itertools import product
import os
import sys
import duckdb
import pandas as pd
import csv
import argparse
from utils import parse_plan_timings, Run, getStats, MicroDataSelective, get_lineage_type

################### Filter ###########################
# predicate: z=0
# vary: cardinality, selectivity
# operators: Filter (FILTER), and Scan with filter push down (SEQ_SCAN)
########################################################
def FilterMicro(con, iter, args, lineage_type, selectivity, cardinality, results, pushdown):
    print(f"----- {lineage_type} ------- Test Filter zipfan 1  filter_pushdown: ", pushdown)
    op_name = "FILTER"
    if pushdown:
        op_name = "SEQ_SCAN"
        con.execute("PRAGMA enable_filter_pushdown")
    else:
        con.execute("PRAGMA disable_filter_pushdown")

    for sel, card in product(selectivity, cardinality):
        args.qid=f"Filter_sel{sel}card{card}"
        sel_str = f"{sel}".replace(".", "_")
  
        perm_rid = ''
        if args.perm:
            perm_rid = 't1.rowid as rid,'

        q = f"SELECT {perm_rid}* FROM micro_table_{sel_str}_{card} as t1 WHERE z=0"
        table_name = None
        print("-------")
        q = "create temp table t1_perm_lineage as "+ q
        table_name='t1_perm_lineage'
        avg, df = Run(q, args, con, table_name)
        df = con.execute("select count(*) as c from t1_perm_lineage").fetchdf()
        output_size = df.loc[0,'c']
        con.execute("drop table t1_perm_lineage")
        
        lineage_size_mb, lineage_count, nchunks, postprocess  = 0, 0, 0, 0
        if args.lineage:
            lineage_size_mb, lineage_count, nchunks, postprocess, plan = getStats(con, q)
        
        plan_timings, plan_full = parse_plan_timings(args.qid)
        results.append({'iter': iter, 'lineage_type': lineage_type, 'op_name': op_name, 'runtime': avg, 'card': card,
            'sel': sel, 'output_size': output_size, 'lineage_size_mb': lineage_size_mb,
            'lineage_count': lineage_count, 'nchunks': nchunks,
            'postprocess': postprocess,
            "plan_timings": str(plan_timings), "plan": str(plan_full), 'notes': args.notes})

        if args.lineage:
            if args.show_tables:
                tables = con.execute("PRAGMA show_tables").fetchdf()
                print(tables)
                if args.lineage:
                    for t in tables['name']:
                        if "LINEAGE" in t:
                            print(t)
                            print(con.execute(f"select * from {t}").df())
            con.execute("PRAGMA clear_lineage")

    con.execute("PRAGMA enable_filter_pushdown")

if False:
    parser = argparse.ArgumentParser(description='TPCH benchmarking script')
    parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
    parser.add_argument('--show_output', action='store_true',  help="Print query output")
    parser.add_argument('--show_tables', action='store_true',  help="Print query output")
    parser.add_argument('--lineage', action='store_true',  help="Enable lineage")
    parser.add_argument('--perm', action='store_true',  help="Enable lineage")
    parser.add_argument('--notes', type=str,  help="exp notes", default="")
    parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
    parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")

    args = parser.parse_args()
    args.profile = True
    args.stats = True

    pd.set_option('display.max_rows', None)

    ################### Filter ###############################
    # Test filtering on values on z varying cardinality (card)
    # and selectivity (sel)
    ##########################################################
    # Creating connection
    db_name = 'filter_micro_db.out'
    selectivity = [0.02] #, 0.2, 0.5, 1.0]
    cardinality = [10000000] #, 5000000, 10000000]
    lineage_type = get_lineage_type(args)
    if not os.path.exists(db_name):
        print("init filter_micro_db.out", selectivity, cardinality)
        con = duckdb.connect(db_name)
        MicroDataSelective(con, selectivity, cardinality)
    else:
        con = duckdb.connect(db_name)

    results = []
    pushdown = False
    FilterMicro(con, 0, args, lineage_type, selectivity, cardinality, results, pushdown)
    pushdown = True
    FilterMicro(con, 0, args, lineage_type, selectivity, cardinality, results, pushdown)
    print(results)

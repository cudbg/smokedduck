from itertools import product
import os
import sys
import duckdb
import pandas as pd
import csv
import argparse
from utils import parse_plan_timings, Run, getStats

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
        if args.mat:
            q = "create temp table t1_perm_lineage as "+ q
            table_name='t1_perm_lineage'
        avg, df = Run(q, args, con, table_name)
        if args.mat:
            df = con.execute("select count(*) as c from t1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table t1_perm_lineage")
        else:
            output_size = len(df)
        
        lineage_size_mb, lineage_count, nchunks, postprocess  = 0, 0, 0, 0
        if args.lineage:
            lineage_size_mb, lineage_count, nchunks, postprocess, _, plan = getStats(con, q)
        
        plan_timings, plan_full = parse_plan_timings(args.qid)
        results.append({'iter': iter, 'lineage_type': lineage_type, 'op_name': op_name, 'runtime': avg, 'card': card,
            'sel': sel, 'output_size': output_size, 'lineage_size_mb': lineage_size_mb,
            'lineage_count': lineage_count, 'nchunks': nchunks,
            'postprocess': postprocess,
            "plan_timings": str(plan_timings), "plan": str(plan_full), 'notes': args.notes})

        if args.lineage:
            con.execute("PRAGMA clear_lineage")

    con.execute("PRAGMA enable_filter_pushdown")

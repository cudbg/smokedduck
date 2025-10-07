from itertools import product
import os
import sys
import duckdb
import pandas as pd
import csv
import argparse
from utils import parse_plan_timings, Run, getStats

def core_aggs(args, con, tname):
    q = f"SELECT z, count(*) as agg FROM {tname} zipf1 GROUP BY z"

    table_name, method = None, ''
    if args.perm and args.group_concat:
        q = f"SELECT z, count(*) as c, group_concat(rowid,',') FROM {tname} zipf1 GROUP BY z"
        method="_group_concat"
    elif args.perm and args.list:
        q = f"SELECT z, count(*) as c , list(rowid) FROM {tname} zipf1 GROUP BY z"
        method="_list"
    elif args.perm and args.window:
        q = f"SELECT rowid as rid, z, count(*) over (partition by z) as c FROM {tname} zipf1"
        method="_window"
    elif args.perm:
        q = f"SELECT zipf1.rowid as rid, z, c FROM (SELECT z, count(*) as c FROM {tname} GROUP BY z) as temp join {tname} zipf1 using (z)"

    if args.mat:
        q = "create temp table zipf1_perm_lineage as "+ q
        table_name='zipf1_perm_lineage'
    avg, df = Run(q, args, con, table_name)
    if args.mat:
        df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
        output_size = df.loc[0,'c']
        con.execute("drop table zipf1_perm_lineage")
    else:
        output_size = len(df)

    return q, output_size, avg, method

################### int Hash Aggregate  ############
##  Group by: 'z' 
## vary: 'g' number of unique values, cardinality, and skew (TODO)
########################################################
def int_hashAgg(con, iter, args, lineage_type, groups, cardinality, results, agg_type):
    print("------------ Test Int Group By zipfan 1, ", lineage_type, agg_type)
    
    # force operators
    if args.window == False and agg_type == "PERFECT_HASH_GROUP_BY":
        op_code = "perfect"
        con.execute(f"PRAGMA set_agg('{op_code}')")
    elif args.window == False and agg_type == "HASH_GROUP_BY":
        op_code = "reg"
        con.execute(f"PRAGMA set_agg('{op_code}')")

    p  = 2
    for g, card in product(groups, cardinality):
        args.qid='gb_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)
        zipf1 = f"zipf_micro_table_1_{g}_{card}"
        #print(con.execute(f"select * from {zipf1}").df())
        print(iter, g, card)
        q, output_size, avg, method = core_aggs(args, con, zipf1)
        lineage_size, lineage_count, nchunks, postprocess  = 0, 0, 0, 0
        if args.lineage:
            lineage_size, lineage_count, nchunks, postprocess, _, plan = getStats(con, q)
        plan_timings, plan_full = parse_plan_timings(args.qid)
        results.append({'iter': iter, 'op_name': agg_type, 'runtime': avg,
            'card': card, 'col': p, 'groups': g,
            'output_size': output_size, 'lineage_size_mb': lineage_size,
            'lineage_count': lineage_count, 'nchunks': nchunks,
            'postprocess': postprocess,
            'lineage_type': str(lineage_type)+method, 'plan_timings': str(plan_timings),
            'plan': str(plan_full), 'notes': args.notes})
        if args.lineage:
            con.execute("PRAGMA clear_lineage")
    con.execute("PRAGMA set_agg('clear')")

################### Hash Aggregate  ############
##  Group by on 'z' with 'g' unique values and table size
#   of 'card'. Test on various 'g' values.
########################################################
def hashAgg(con, iter, args, lineage_type, groups, cardinality, results):
    print("------------ Test Group By zipfan 1, ", lineage_type)
    p = 2
    con.execute(f"PRAGMA set_agg('reg')")
    for g, card in product(groups, cardinality):
        args.qid='gb_ltype{}g{}card{}p{}'.format(lineage_type,g, card, p)
        zipf1 = f"zipf_varchar_micro_table_1_{g}_{card}"
        # zipf1 = zipf1.astype({"z": str})
        print(iter, g, card)
        q = f"SELECT z, count(*) FROM {zipf1} zipf1 GROUP BY z"
        table_name, method = None, ''
        if args.perm and args.group_concat:
            q = f"SELECT z, count(*), group_concat(rowid,',') FROM {zipf1} zipf1 GROUP BY z"
            method="_group_concat"
        elif args.perm and args.list:
            q = f"SELECT z, count(*), list(rowid) FROM {zipf1} GROUP BY z"
            method="_list"
        elif args.perm:
            q = f"SELECT zipf1.rowid, z FROM (SELECT z, count(*) FROM {zipf1} GROUP BY z) as temp join {zipf1} zipf1 using (z)"
        if args.mat:
            q = "create temp table zipf1_perm_lineage as "+ q
            table_name='zipf1_perm_lineage'
        avg, df = Run(q, args, con, table_name)
        if args.mat:
            df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
            output_size = df.loc[0,'c']
            con.execute("drop table zipf1_perm_lineage")
        else:
            output_size = len(df)
        lineage_size, lineage_count, nchunks, postprocess  = 0, 0, 0, 0
        if args.lineage:
            lineage_size, lineage_count, nchunks, postprocess, _, plan = getStats(con, q)
        plan_timings, plan_full = parse_plan_timings(args.qid)
        results.append({'iter': iter, 'op_name': 'HASH_GROUP_BY_var', 'runtime': avg,
            'card': card, 'col': p, 'groups': g,
            'output_size': output_size, 'lineage_size_mb': lineage_size,
            'lineage_count': lineage_count, 'nchunks': nchunks,
            'postprocess': postprocess,
            'lineage_type': str(lineage_type)+method, 'plan_timings': str(plan_timings),
            'plan': str(plan_full), 'notes': args.notes})
        if args.lineage:
            con.execute("PRAGMA clear_lineage")
    con.execute("PRAGMA set_agg('clear')")

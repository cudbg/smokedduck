from itertools import product
import random
import os
import sys
import duckdb
import pandas as pd
import numpy as np
import csv
import argparse
from utils import Run, getStats, MicroDataZipfan, parse_plan_timings, get_lineage_type

def core_mtn(con, args, op, table1, table2):
    perm_rid = ''
    if args.perm:
        perm_rid = "zipf1.rowid as zipf1_rowid, zipf2.rowid as zipf2_rowid,"
    q = f"SELECT {perm_rid}zipf2.idx, zipf1.v FROM {table1} zipf1, {table2} zipf2 WHERE zipf1.z=zipf2.z"
    table_name = None
    q = "create table perm_lineage as "+ q
    table_name='perm_lineage'
    avg, df = Run(q, args, con, table_name)
    df = con.execute("select count(*) as c from perm_lineage").fetchdf()
    output_size = df.loc[0,'c']
    con.execute("drop table perm_lineage")

    return q, output_size, avg

def MtM(con, iter, args, lineage_type, groups, cardinality, a_list, results, op, varchar=""):
    print("------------ Test M:N ", op)
    # SELECT * FROM zipf1,zipf2 WHERE zipf1.z=zipf2.z
    # zipfian distributions (θ = 1)
    # zipf1.z is within [1,10] or [1, 100] while zipf2.z∈ [1, 100]
    # This means that tuples with z = 1 have a disproportionately large number of
    # matches compared to larger z values that have fewer matches.
    # For this experiment, we also fix the size of the left table zipf1 to 10^3 records
    # and vary the right zipf2 from 10^3 to 10^5

    p = 2
    n1 = 1000
    con.execute(f"PRAGMA set_join('{op}')")
    for a, g, card in product(a_list, groups, cardinality):
        g2 = g
        # g either 10 or 100
        a_str = f'{a}'.replace('.', '_')
        table1 = f"zipf_{varchar}micro_table_{a_str}_{g}_{n1}"
        table2 = f"zipf_{varchar}micro_table_1_{g2}_{card}"
        args.qid=f'ineq_ltype{lineage_type}g{g}card{card}p{p}a{a_str}'
        q, output_size, avg = core_mtn(con, args, op, table1, table2)
        sel = float(output_size) / (card*n1)
        print("\n", avg, output_size, sel)
        
        lineage_size_mb, lineage_count, nchunks, postprocess  = 0, 0, 0, 0
        if args.lineage:
            lineage_size_mb, lineage_count, nchunks, postprocess, plan = getStats(con, q)
        plan_timings, plan_full = parse_plan_timings(args.qid)
        results.append({'iter': iter, 'op_name': f'{op}_mtm{varchar}', 'runtime': avg, 'n1': n1, 'n2': card,
            'sel': sel, 'skew': a, 'ncol': p, 'groups': g,
            'output_size': output_size, 'lineage_size_mb': lineage_size_mb,
            'lineage_count': lineage_count, 'nchunks': nchunks,
            'postprocess': postprocess, 'lineage_type': lineage_type,
            'plan_timings': str(plan_timings), 'notes': args.notes})
        if args.lineage:
            if args.show_tables:
                tables = con.execute("PRAGMA show_tables").fetchdf()
                print(tables)
                if args.lineage:
                    for t in tables['name']:
                        if "LINEAGE" in t and "SCAN" not in t:
                            print(t)
                            print(con.execute(f"select * from {t}").df())
            con.execute("PRAGMA clear_lineage")
    con.execute(f"PRAGMA set_join('clear')")

def setup_pt(con, g, op, varchar):
    # PT(id int, v int)
    # cardinality = g
    if len(varchar) > 0:
        idx = [f'Number_{num}' for num in list(range(0, g))]
    else:
        idx = list(range(0, g))
    vals = np.random.uniform(0, 100, g)
    PT = pd.DataFrame({'id':idx, 'v':vals})
    con.execute("create table PT as select * from PT")

def core_pkfk(con, args, op, ft_table):
    perm_rid = ''
    if args.perm:
        perm_rid = "FT.rowid as FT_rowid, PT.rowid as PT_rowid,"
    q = f"""SELECT {perm_rid}FT.v, PT.id FROM PT , {ft_table} as FT WHERE PT.id=FT.z"""
    table_name = None
    q = "create table perm_lineage as "+ q
    table_name='perm_lineage'
    avg, df = Run(q, args, con, table_name)
    df = con.execute("select count(*) as c from perm_lineage").fetchdf()
    output_size = df.loc[0,'c']
    con.execute("drop table perm_lineage")

    return q, output_size, avg

def FKPK(con, iter, args, lineage_type, groups, cardinality, a_list, results, op, varchar=""):
    # SELECT * FROM gids,zipf WHERE gids.id=zipf.z. zipf.z 
    # zipf.z is a foreign key that references gids.id and 
    # is drawn from a zipfian distribution (θ = 1) 
    # We vary the number of join matches by varying the unique values for gids.id
    # g = (100, 10000)
    # n = 1M, 5M, 10M
    print("------------ Test FK-PK ", op, groups, cardinality)
    con.execute(f"PRAGMA set_join('{op}')")
    p = 2
    for g in groups:
        setup_pt(con, g, op, varchar)
        for a, card in product(a_list, cardinality):
            a_str = f'{a}'.replace('.', '_')
            ft_table = f"zipf_{varchar}micro_table_{a_str}_{g}_{card}"
            args.qid=f"ineq_ltype{lineage_type}g{g}card{card}p{p}a{a_str}"
            q, output_size, avg_runtime = core_pkfk(con, args, op, ft_table)
            print(avg_runtime, output_size)
            lineage_size_mb, lineage_count, nchunks, postprocess  = 0, 0, 0, 0
            if args.lineage:
                lineage_size_mb, lineage_count, nchunks, postprocess, plan = getStats(con, q)
            plan_timings, plan_full = parse_plan_timings(args.qid)
            results.append({'iter': iter, 'op_name': f'{op}{varchar}', 'runtime': avg_runtime, 'n1': card,
                'n2': g, 'sel': -1, 'skew': a, 'ncol': p, 'groups': g,
                'output_size': output_size, 'lineage_size_mb': lineage_size_mb,
                'lineage_count': lineage_count, 'nchunks': nchunks,
                'postprocess': postprocess,
                'lineage_type': lineage_type, 'plan_timings': str(plan_timings), 'notes': args.notes})
            if args.lineage:
                if args.show_tables:
                    tables = con.execute("PRAGMA show_tables").fetchdf()
                    print(tables)
                    if args.lineage:
                        for t in tables['name']:
                            if "LINEAGE" in t and "SCAN" not in t:
                                print(t)
                                print(con.execute(f"select * from {t}").df())
                con.execute("PRAGMA clear_lineage")
        con.execute("drop table PT")
    con.execute(f"PRAGMA set_join('clear')")

def setup_join_lessthan(con, card, sel, p):
    """
    t1(v, id, col1, .., colp)
    t2(v, id, col1, .., colp)
    
    len(t1) = card[0]
    len(t2) = card[1]

    t1 JOIN t2 ON (v) --> selectivity of the join condition = sel

    TODO: divide selectivity failures between the two tables
    add values to t1 that fails t1.v < t2.v --> t1.v > t2.v
    add values to t2 that fails t1.v < t2.v --> t2.v < t1.v
    
    n1[fail, succuess ] < n2[fail, success]
    n1[fail] = [10n1 .. 20n1]
    n1[success] = 2n1

    n2[fail] = [0..n1]
    n2[success] = 3n1
    """
    print("sel: ", sel, "card", card, "p", p)
    # create tables & insert values
    n1 = card[0]
    n2 = card[1]
    expected_output = sel * n1 * sel * n2
    k1 = (sel * n1) # seccuss
    k2 = (sel * n2) # success
    print(f"Matching rows for {sel} * {n1} * {sel} * {n2} = {expected_output} {k1} {k2}")
    ## Table 1, len(v1) = n1, random values
    ## that would fail the join condition
    v1 = np.random.uniform(10*n1, 20*n1, n1)
    # pick k random indexes between 0 and n1
    # replace them with values that would eval to true
    IDX = random.sample(range(n1), int(k1))
    v1[IDX] = 2*n1

    print("done generating data v1")
    ## Table 2, len(v2) = n2, random values
    ## that would fail the join condition
    v2 = np.random.uniform(0, n1, n2)
    # pick k random indexes between 0 and n2
    # replace them with values that would eval to true
    IDX = random.sample(range(n2), int(k2))
    v2[IDX] = 3*n1
    print("done generating data v2")
    
    idx1 = list(range(0, n1))
    idx2 = list(range(0, n2))
    t1 = pd.DataFrame({'v':v1, 'id':idx1})
    t2 = pd.DataFrame({'v':v2, 'id':idx2})
    for col in range(p):
        t1["col{}".format(col)]  = np.random.randint(0, 100, len(t1))
        t2["col{}".format(col)]  = np.random.randint(0, 100, len(t2))
    con.execute("create table t1 as select * from t1")
    con.execute("create table t2 as select * from t2")
    print("done generating data")

def core_join_less(con, args, pred):
    # Run query
    perm_rid = ''
    if args.perm:
        perm_rid = "t1.rowid as r1_rowid, t2.rowid as t2_rowid, "
    q = f"select {perm_rid}* from t1, t2{pred}"
    table_name = None
    q = "create table zipf1_perm_lineage as "+ q
    table_name='zipf1_perm_lineage'
    avg, df = Run(q, args, con, table_name)
    df = con.execute("select count(*) as c from zipf1_perm_lineage").fetchdf()
    output_size = df.loc[0,'c']
    con.execute("drop table zipf1_perm_lineage")
    return q, output_size, avg


def join_lessthan(con, iter, args, lineage_type, cardinality, results, op, force_join, pred, sels=[0.0]):
    print("------------ Test Join  ", op, pred, force_join)
    if (force_join):
        op_code = "nl"
        if op == "PIECEWISE_MERGE_JOIN":
            op_code = "merge"
        elif op == "BLOCKWISE_NL_JOIN":
            op_code = "bnl"
        elif op == "CROSS_PRODUCT":
            op_code = "cross"
        elif op == "NESTED_LOOP_JOIN":
            op_code = "nl"
        else:
            print("ERROR ", op)
        con.execute(f"PRAGMA set_join('{op_code}')")
    projections = [0] 
    for sel, card, p in product(sels, cardinality, projections):
        
        setup_join_lessthan(con, card, sel, p)

        args.qid='ineq_ltype{}g{}card{}p{}'.format(lineage_type,sel, card, p)
        
        q, output_size, runtime_avg = core_join_less(con, args, pred)
        print(runtime_avg, output_size)

        lineage_size_mb, lineage_count, nchunks, postprocess  = 0, 0, 0, 0
        if args.lineage:
            lineage_size_mb, lineage_count, nchunks, postprocess, plan = getStats(con, q)
        plan_timings, plan_full = parse_plan_timings(args.qid)
        results.append({'iter': iter, 'op_name': op, 'runtime': runtime_avg, 'n1': card[0],
            'n2': card[1], 'sel': sel, 'skew': -1, 'ncol': p, 'groups': -1,
            'output_size': output_size, 'lineage_size_mb': lineage_size_mb,
            'lineage_count': lineage_count, 'nchunks': nchunks,
            'postprocess': postprocess,
            'lineage_type': lineage_type, 'plan_timings': str(plan_timings), 'notes': args.notes})
        if args.lineage:
            if args.show_tables:
                tables = con.execute("PRAGMA show_tables").fetchdf()
                print(tables)
                if args.lineage:
                    for t in tables['name']:
                        if "LINEAGE" in t and "SCAN" not in t:
                            print(t)
                            print(con.execute(f"select * from {t}").df())
            con.execute("PRAGMA clear_lineage")
        con.execute("drop table t1")
        con.execute("drop table t2")
    con.execute("PRAGMA set_join('clear')")


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

    results = []
    db_name = 'join_micro_db.out'
    lineage_type = get_lineage_type(args)
    groups = [10000] #, 100000]
    cardinality = [1000000] #, 5000000, 10000000]
    a_list = [0] #, 1]
    if not os.path.exists(db_name):
        print("get data")
        con = duckdb.connect(db_name)
        MicroDataZipfan(con, groups, cardinality,  a_list)
    else:
        con = duckdb.connect(db_name)

    op = "HASH_JOIN"
    FKPK(con, 0, args, lineage_type, groups, cardinality, a_list, results, op)
    MtM(con, 0, args, lineage_type, groups, cardinality, a_list, results, op)

    ################### Inequality Joins  ############
    # 1k (10k, 100k, 1M)
    cardinality = [(1000, 10000)] #, (1000, 100000), (1000, 1000000)]
    sels = [0.1] #, 0.2, 0.5, 0.8]
    pred = " where t1.v < t2.v"
    op = "NESTED_LOOP_JOIN"
    join_lessthan(con, 0, args, lineage_type, cardinality, results, op, True, pred, sels)

    op = "PIECEWISE_MERGE_JOIN"
    join_lessthan(con, 0, args, lineage_type, cardinality, results, op, True, pred, sels)

    pred = " where t1.v = t2.v or t1.v < t2.v"
    op = "BLOCKWISE_NL_JOIN"
    join_lessthan(con, 0, args, lineage_type, cardinality, results, op, False, pred, sels)

    pred = ""
    op = "CROSS_PRODUCT"
    join_lessthan(con, 0, args, lineage_type, cardinality, results, op, False, pred)


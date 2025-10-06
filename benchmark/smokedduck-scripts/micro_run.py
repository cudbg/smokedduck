from itertools import product
import os
import sys
import duckdb
import pandas as pd
import csv
import argparse
from utils import MicroDataSelective, MicroDataZipfan, get_lineage_type
from filter_micro import FilterMicro
from agg_micro import hashAgg, int_hashAgg
from join_micro import join_lessthan, FKPK, MtM

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
parser.add_argument('--show_output', action='store_true',  help="Print query output")
parser.add_argument('--show_tables', action='store_true',  help="Print query output")
parser.add_argument('--lineage', action='store_true',  help="Enable lineage")
parser.add_argument('--perm', action='store_true',  help="Enable lineage")
parser.add_argument('--smoke', action='store_true',  help="Enable Smoke")
parser.add_argument('--notes', type=str,  help="exp notes", default="")
parser.add_argument('--save', action='store_true',  help="save result in csv")
parser.add_argument('--run_filter', action='store_true',  help="eval filter")
parser.add_argument('--run_ineq', action='store_true',  help="eval ineq join")
parser.add_argument('--run_hj', action='store_true',  help="eval hash join fkpk")
parser.add_argument('--run_hj_mtn', action='store_true',  help="eval hash join mtn")
parser.add_argument('--run_agg', action='store_true',  help="eval agg")
parser.add_argument('--mat', action='store_true',  help="store in lineage table")

args = parser.parse_args()
args.profile = True
args.stats = True
lineage_type = get_lineage_type(args)


pd.set_option('display.max_rows', None)

run_filter = args.run_filter
run_ineq = args.run_ineq
run_hj = args.run_hj
run_hj_mtn = args.run_hj_mtn
run_agg = args.run_agg

filter_results = []
join_results = []
agg_results = []

################### Filter ###############################
# Test filtering on values on z varying cardinality (card)
# and selectivity (sel)
##########################################################
# Creating connection
############################################
# Q = select * from T where z=0
# T(idx int, z int, col float)
# |T| in {1M, 5M, 10M}
# sel in {0.02, 0.2, 0.5, 1.0}
############################################

if run_filter:
    selectivity = [0.02, 0.2, 0.5, 1.0]
    cardinality = [1000000, 5000000, 10000000]
    setting = [False, True]
    for pushdown in setting:
        for sel, card in product(selectivity, cardinality):
            for r in range(args.repeat):
                db_name = 'filter_micro_db.out'
                if not os.path.exists(db_name):
                    print("init filter_micro_db.out", selectivity, cardinality)
                    con = duckdb.connect(db_name)
                    MicroDataSelective(con, selectivity, cardinality)
                else:
                    con = duckdb.connect(db_name)

                con.execute("PRAGMA threads=1")
                con.execute("drop table if exists t1_perm_lineage")
                
                FilterMicro(con, r, args, lineage_type, [sel], [card], filter_results, pushdown)
                con.close()

################### Aggregation ###############################
##########################################################

def VarCharZipfan(con, groups, cardinality, a_list):
    # iterate over all zipf tables and consruct one with varchar z
    for a, g, card in product(a_list, groups, cardinality):
        a_str = f'{a}'.replace('.', '_')
        zipf1 = f"zipf_micro_table_{a_str}_{g}_{card}"
        df = con.execute(f"select * from {zipf1}").df()
        mapping = {num: f'Number_{num}' for num in df['z'].unique()}
        df['z'] = df['z'].map(mapping)
        con.execute(f"""create table zipf_varchar_micro_table_{a_str}_{g}_{card} as select * from df""")


############################################
# Q = select z, sum(v) from T group by z
# T = zipf1,g(id int, z int, b float)
# |T| in {1M, 5M, 10M}
# g in {10, 100, 1000}
############################################
if run_agg:
    groups = [10, 100, 1000]
    cardinality = [10000000, 5000000, 1000000]
    a_list = [1]
    settings =  [[False, False, False,  "HASH_GROUP_BY"],
            [False, False, False,  "VAR"],
            [False, False, False,  "PERFECT_HASH_GROUP_BY"],
         ]

    if args.perm:
        settings.append([True, False, False,  "HASH_GROUP_BY"])
        settings.append([True, False, False,  "PERFECT_HASH_GROUP_BY"])
        settings.append([True, False, False,  "VAR"])
        settings.append([False, False, True,  "HASH_GROUP_BY"])
        settings.append([False, True, False,  "HASH_GROUP_BY"])

    for s in settings:
        for g, card in product(groups, cardinality):
            for r in range(args.repeat):
                db_name = 'micro_agg_db_v2.out'
                if not os.path.exists(db_name):
                    print("construct micro_agg_db.out", groups, cardinality, a_list)
                    con = duckdb.connect(db_name)
                    MicroDataZipfan(con, groups, cardinality, a_list)
                    VarCharZipfan(con, groups, cardinality, a_list)
                else:
                    con = duckdb.connect(db_name)

                con.execute("PRAGMA threads=1")

                args.list = s[0]
                args.window = s[1]
                args.group_concat = s[2]
                agg_type = s[3]
                if agg_type == "VAR":
                    hashAgg(con, r, args, lineage_type, [g], [card], agg_results)
                else:
                    int_hashAgg(con, r, args, lineage_type, [g], [card], agg_results, agg_type)
                con.close()

################### Inequality Joins  ############
# Q = select * from T1, T2 where T1.v < T2.v
# T1(id int, v float), T2(id int, v float)
# |T1|=1K, |T2| in {10K, 100K, 1M}
# s in {0.2, 0.5, 0.8}
############################################
# 1k (10k, 100k, 1M)
if run_ineq:
    cardinality = [(1000, 10000), (1000, 100000), (1000, 1000000)]
    sels = [0.1, 0.2, 0.5, 0.8]
    
    preds = [" where t1.v < t2.v", " where t1.v < t2.v", " where t1.v = t2.v or t1.v < t2.v", ""]
    ops = ["NESTED_LOOP_JOIN", "PIECEWISE_MERGE_JOIN", "BLOCKWISE_NL_JOIN", "CROSS_PRODUCT"]
    flags = [True, True, False, False]

    for pred, op, f in zip(preds, ops, flags): 
        for r in range(args.repeat):
            db_name = f'temp_{op}_{r}.out' #join_micro_db.out'
            if op == "CROSS_PRODUCT":
                for card in cardinality:
                    con = duckdb.connect(db_name)
                    join_lessthan(con, r, args, lineage_type, [card], join_results, op, f, pred)
                    con.close()
            else:
                for sel, card, in product(sels, cardinality):
                    con = duckdb.connect(db_name)
                    join_lessthan(con, r, args, lineage_type, [card], join_results, op, f, pred, [sel])
                    con.close()
################### Join ###################
# Q = select z, sum(v) from T group by z
# T = zipf1,g(id int, z int, b float)
# |T| in {1M, 5M, 10M}
# g in {10, 100, 1000}
############################################
if run_hj:
    groups = [10000, 100000]
    cardinality = [100000, 1000000, 5000000]
    a_list = [0, 1]
    settings = ["", "varchar_"]
    for s in settings:
        for r in range(args.repeat):
            db_name = 'join_micro_db.out'
            if not os.path.exists(db_name):
                print("construct", db_name, groups, cardinality, a_list)
                con = duckdb.connect(db_name)
                MicroDataZipfan(con, groups, cardinality,  a_list)
                VarCharZipfan(con, groups, cardinality, a_list)
            else:
                con = duckdb.connect(db_name)
                print(con.execute("pragma show_tables").df())

            con.execute("PRAGMA threads=1")
            con.execute("drop table if exists PT")

            op = "HASH_JOIN"
            # run one with varchar keys
            FKPK(con, r, args, lineage_type, groups, cardinality, a_list, join_results, op, s)

if run_hj_mtn:
    settings = ["varchar_", ""]
    for s in settings:
        for r in range(args.repeat):
            db_name = 'join_mtn_micro_db.out'
            groups = [5, 10, 100]
            a_list = [0, 0.5, 0.8, 1]
            cardinality = [10000, 100000, 1000000]
            cardinality = [10000, 1000000]
            op = "HASH_JOIN"
            for a, g, card in product(a_list, groups, cardinality):
                if not os.path.exists(db_name):
                    con = duckdb.connect(db_name)
                    groups = [5, 10, 100]
                    a_list = [0, 0.5, 0.8, 1]
                    MicroDataZipfan(con, [g], [1000],  [a])
                    VarCharZipfan(con, [g], [1000], [a])
                    
                    cardinality = [10000, 100000, 1000000]
                    MicroDataZipfan(con, [g], [card],  [a])
                    VarCharZipfan(con, [g], [card], [a])
                else:
                    con = duckdb.connect(db_name)
                    con.execute("drop table if exists perm_lineage")

                con.execute("PRAGMA threads=1")
                print(con.execute("PRAGMA metadata_info").df())

                MtM(con, r, args, lineage_type, [g], [card], [a], join_results, op, s)
                con.close()


if args.save:
    df_filter = pd.DataFrame(filter_results)
    print(df_filter)
    df_join = pd.DataFrame(join_results)
    print(df_join)
    df_agg = pd.DataFrame(agg_results)
    print(df_agg)
    dbname=f"micro_benchmark_{args.notes}.out"
    print(dbname)

    if not os.path.exists(db_name):
        con = duckdb.connect(dbname)
        if len(df_filter) > 0:
            con.execute(f"""create or replace table filter_results as select * from df_filter""")
        
        if len(df_join) > 0:
            con.execute(f"""create or replace table join_results as select * from df_join""")
        
        if len(df_agg) > 0:
            con.execute(f"""create or replace table agg_results as select * from df_agg""")
    else:
        con = duckdb.connect(dbname)
        if len(df_filter) > 0:
            con.execute(f"""create table if not exists filter_results as select * from df_filter where 1=0""")
            con.execute("INSERT INTO filter_results SELECT * FROM df_filter")

        if len(df_join) > 0:
            con.execute(f"""create table if not exists join_results as select * from df_join where 1=0""")
            con.execute("INSERT INTO join_results SELECT * FROM df_join")
        
        if len(df_agg) > 0:
            con.execute(f"""create table if not exists agg_results as select * from df_agg where 1=0""")
            con.execute("INSERT INTO agg_results SELECT * FROM df_agg")

import ast
import json
import pandas as pd
import argparse
from pygg import *
import duckdb
from duckdb.typing import *
from utils import legend_bottom, legend_side, relative_overhead, overhead, getAllExec, getMat, getTime

type1 = [1, 3, 5, 6, 7, 8, 9, 10, 12, 13, 14, 19]
type2 = [11, 15, 16, 18]
type3 = [2, 4, 17, 20, 21, 22]

def cat(qid):
    if int(qid) in type1:
        return "1. Joins-Aggregations"
    elif int(qid) in type2:
        return "2. Uncorrelated subQs"
    else:
        return "3. Correlated subQs"

class_list = type1
class_list.extend(type2)
class_list.extend(type3)
queries_order = [""+str(x)+"" for x in range(1,len(class_list)+1)]
queries_order = [""+str(x)+"" for x in class_list]
queries_order = ','.join(queries_order)


parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--db', type=str, help='queries folder', default='tpch_benchmark_capture_may27_e.csv')
args = parser.parse_args()


con = duckdb.connect(args.db)
con.create_function("getMat", getMat, [VARCHAR], FLOAT)
con.create_function("getTime", getTime, [VARCHAR], FLOAT)
con.create_function("getAllExec", getAllExec, [VARCHAR], FLOAT)
con.create_function("cat", cat, [BIGINT], VARCHAR)
print(con.execute("select * from tpch_capture").df())
#getTime(plan) as plan_runtime
tpch_df = con.execute("""select *, cat(query) as qtype, getMat(plan_timings) as mat_time,
        getAllExec(plan_timings) as plan_runtime
    from tpch_capture""").df()
tpch_opt = con.execute("""select * from tpch_df
                    where lineage_type='Logical-RID'
                          and query not in (select query from tpch_df where lineage_type='Logical-OPT')
                   """).fetchdf()
tpch_opt['lineage_type'] = 'Logical-OPT'
tpch_all = con.execute("""select * from tpch_df UNION ALL
                          select * from tpch_opt
                        """).fetchdf()
header = tpch_all.columns.tolist()
print(header)
header_unique = ["query","sf", "qtype", "lineage_type", "n_threads"]
metrics = ["runtime", "output", "mat_time", "plan_runtime", "lineage_size", "lineage_count", "nchunks", "postprocess_time"]
g = ','.join(header_unique)
m = ','.join(metrics)
avg_tpch = con.execute("""select {},
                            max(nchunks) as nchunks,
                            max(lineage_size) as lineage_size, max(lineage_count) as lineage_count,
                            avg(postprocess_time) as postprocess_time,
                            avg(plan_runtime) as plan_runtime, avg(runtime) as runtime,
                            avg(output) as output,  avg(mat_time) as mat_time from tpch_all
                            group by {}""".format(g, g)).fetchdf()
con.execute("INSTALL JSON")
con.execute("LOAD JSON")
print(con.execute("select plan from tpch_all").df())


def recurse(plan, res=None):
    if res is None:
        res = []
    for c in plan.get('children', []):
        op_name = c.get('name', 'X').strip()
        timing = float(c.get('timing', 0))
        rows = int(c.get('cardinality', 0))
        res.append({'t': timing, 'c': rows, 'n': op_name})
        recurse(c, res)
    return res

def parsePlan(s):
    plan = ast.literal_eval(s)
    timings_card = recurse(plan)
    # Return valid JSON string (with double quotes)
    return json.dumps(timings_card)



con.create_function("parsePlan", parsePlan, [VARCHAR], VARCHAR)
print(con.execute("SELECT parsePlan(plan) AS j FROM tpch_all where n_threads=1 and sf=10").df())
parsedPlan = con.execute("""SELECT ROW_NUMBER() over () as uid, sf, n_threads, query, system,
                unnest.value['t']::DOUBLE AS time,
                unnest.value['c']::BIGINT AS cardinality,
                unnest.value['n']::VARCHAR AS operator
        FROM ( SELECT query, sf, n_threads, lineage_type as system, parsePlan(plan) AS j FROM tpch_all where n_threads=1) t
        CROSS JOIN UNNEST( json_transform(t.j, '[{"t": "DOUBLE", "c": "BIGINT", "n": "VARCHAR"}]') ) AS unnest(value)""").df()

acc = con.execute("""select uid, sf, n_threads, query, system, operator, sum(time) as time, sum(cardinality) as cardinality from parsedPlan
group by uid, sf, n_threads, query, system, operator""").df()

denplan = con.execute("""select sf, n_threads, query || '_' || system as label, query, system, operator, avg(time) as time, avg(cardinality) as cardinality from acc
group by sf, n_threads, query, system, operator""").df()
print(denplan)


q = """
select sf, n_threads, ref.label, query, ref.system, operator, ref.time-COALESCE(base.time, 0) as overhead, COALESCE(base.time,0), ref.time, ref.cardinality
from denplan as ref LEFT JOIN (select * from denplan where system='Baseline') as base
USING (sf, n_threads, query, operator)
where ref.system<>'Baseline' and overhead>0 order by overhead desc
"""
overhead_per_q_per_op = con.execute(q).df()
#(select sf, n_threads, query, sum(time) as total_time from denplan where system='Baseline' group by sf, n_threads, query) as base
print(overhead_per_q_per_op)
#and ref.system='SD_Capture'
q = """
select sf, n_threads, ref.label, query, ref.system, ref.operator, overhead, total_time, (overhead / total_time) * 100 as roverhead, ref.cardinality
from overhead_per_q_per_op as ref LEFT JOIN 
(select sf, n_threads, query, sum(plan_runtime) as total_time from avg_tpch where lineage_type='Baseline' group by sf, n_threads, query) as base
USING (sf, n_threads, query)
where ref.system<>'Baseline' 
and roverhead > 1 order by roverhead desc 
"""
roverhead_per_q_per_op = con.execute(q).df()
print(roverhead_per_q_per_op)
# show operators source of overhead per system per query
# TODO: sum partition by operator, uid, sf, n_threads, query, system 
# TODO: avg partition by operator, sf, n_threads, query, system 

y_axis_list = ["roverhead", "overhead"]
header = ["Relative \nOverhead %", "Overhead (ms)"]
for idx, y_axis in enumerate(y_axis_list):
    p = ggplot(roverhead_per_q_per_op, aes(x='query', ymin=0, ymax=y_axis,  y=y_axis, color='operator', fill='operator', group='operator'))
    p += geom_bar(stat=esc('identity'), alpha=0.8)
    p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10") #", ykwargs=dict(breaks=[20, 100, 1000], labels=list(map(esc, ['20', '100', '1000']))))
    p += geom_hline(yintercept=20, linetype=esc("dotted"))
    p += legend_side
    p += facet_grid(".~sf~n_threads~system", scales=esc("free_x"), space=esc("free_x"))
    postfix = """data$query= factor(data$query, levels=c({}))""".format(queries_order)
    ggsave("figures/tpch_micro_{}.png".format(y_axis), p, postfix=postfix,  width=25, height=10, scale=0.8)
# plot: query, system, operator

# show operators source of overhead per system 
# TODO: sum partition by operator, sf, n_threads, system
summary_per_query = con.execute('select query, system, sf, sum(roverhead) as roverhead, sum(overhead) from roverhead_per_q_per_op group by sf, query, system order by roverhead desc').df()
print(summary_per_query)

summary_per_system = con.execute("""select system, operator, sf, n_threads, sum(roverhead) as roverhead, sum(overhead) as overhead
from roverhead_per_q_per_op group by sf, n_threads, operator, system order by system, sf, roverhead desc""").df()

y_axis_list = ["roverhead", "overhead"]
header = ["Relative \nOverhead %", "Overhead (ms)"]
for idx, y_axis in enumerate(y_axis_list):
    p = ggplot(summary_per_system, aes(x='system', ymin=0, ymax=y_axis,  y=y_axis, color='operator', fill='operator', group='operator'))
    p += geom_bar(stat=esc('identity'), alpha=0.8)
    p += axis_labels('System', "{} (log)".format(header[idx]), "discrete", "log10")
    p += legend_side
    p += facet_grid(".~sf~n_threads", scales=esc("free_x"), space=esc("free_x"))
    ggsave("figures/tpch_micro_per_sys_{}.png".format(y_axis), p, width=10, height=6, scale=0.8)

print(summary_per_system.to_string())

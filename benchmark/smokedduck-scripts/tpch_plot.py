import json
import pandas as pd
import argparse
from pygg import *
import duckdb
from duckdb.typing import *
from utils import legend_bottom, legend_side, relative_overhead, overhead, getAllExec, getMat

type1 = ['1', '3', '5', '6', '7', '8', '9', '10', '12', '13', '14', '19']
type2 = ['11', '15', '16', '18']
type3 = ['2', '4', '17', '20', '21', '22']

def cat(qid):
    if qid in type1:
        return "1. Joins-Aggregations"
    elif qid in type2:
        return "2. Uncorrelated subQs"
    else:
        return "3. Correlated subQs"


parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--db', type=str, help='queries folder', default='tpch_benchmark_capture_may27_e.csv')
args = parser.parse_args()


con = duckdb.connect(args.db)
con.create_function("getMat", getMat, [VARCHAR], FLOAT)
con.create_function("getAllExec", getAllExec, [VARCHAR], FLOAT)
con.create_function("cat", cat, [BIGINT], VARCHAR)
print(con.execute("select * from tpch_capture").df())
tpch_df = con.execute("""select *, cat(query) as qtype, getMat(plan_timings) as mat_time, getAllExec(plan_timings) as plan_runtime
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
header_unique.remove("lineage_type")
g = ','.join(header_unique)

tpch_withbaseline = con.execute(f"""select
                  t1.plan_runtime as base_plan_runtime, t1.runtime as base_runtime,
                  t1.output as base_output, t1.mat_time as base_mat_time,
                  (t1.plan_runtime-t1.mat_time) as base_plan_no_create,
                  (t2.plan_runtime-t2.mat_time) as plan_no_create,
                  t2.* from (select {g}, {m} from avg_tpch where lineage_type='Baseline') as t1
                  join avg_tpch  as t2 using ({g})
                  """).fetchdf()
#print(avg_tpch)
#print(tpch_withbaseline)

tpch_metrics = con.execute("""
select {}, lineage_type, n_threads, output / base_output as fanout, output, nchunks, lineage_size, lineage_count, postprocess_time,
(plan_no_create-base_plan_no_create)*1000 as exec_overhead,
((plan_no_create-base_plan_no_create)/base_plan_no_create)*100 as exec_roverhead,
(mat_time - base_mat_time)*1000 as mat_overhead,
((mat_time - base_mat_time) / base_plan_no_create) *100 as mat_roverhead,
(plan_runtime-base_plan_runtime)*1000 as overhead,
((plan_runtime-base_plan_runtime)/base_plan_no_create)*100 as roverhead,
from tpch_withbaseline order by qtype, query, n_threads, lineage_type
                  """.format(g, g, g)).fetchdf()
print(tpch_metrics)

class_list = type1
class_list.extend(type2)
class_list.extend(type3)
queries_order = [""+str(x)+"" for x in class_list]
queries_order = ','.join(queries_order)

def mktemplate(overheadType, prefix, table):
    return f"""
    SELECT '{overheadType}' as overheadType, qtype,
            query as qid, sf, n_threads, output,
            nchunks, lineage_size, lineage_count, postprocess_time,
           lineage_type as system,
           greatest(0, {prefix}overhead) as overhead, greatest(0, {prefix}roverhead) as roverhead
    FROM {table}"""

template = f"""
  WITH temp as (
    {mktemplate('Total', '', 'tpch_metrics')}
    UNION ALL
    {mktemplate('Materialize', 'mat_', 'tpch_metrics')}
    UNION ALL
    {mktemplate('Execute', 'exec_', 'tpch_metrics')}
  ) SELECT * FROM temp {"{}"} ORDER BY overheadType desc """

where = f"where overheadtype<>'Materialize' and n_threads=1"
q = template.format(where)
print(q)
data = con.execute(q).fetchdf()
if 1:
    y_axis_list = ["roverhead", "overhead"]
    header = ["Relative \nOverhead %", "Overhead (ms)"]
    for idx, y_axis in enumerate(y_axis_list):
        p = ggplot(data, aes(x='qid', ymin=0, ymax=y_axis,  y=y_axis, color='system', fill='system', group='system', shape='overheadType'))
        p += geom_point(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.5, size=2)
        p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.8)
        if y_axis == 'overhead':
            p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[10, 100, 1000], labels=list(map(esc, ['10', '100', '1000']))))
        else:
            p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[20, 100, 1000], labels=list(map(esc, ['20', '100', '1000']))))
            p += geom_hline(aes(yintercept=20, linetype=esc("dotted")))
            p += geom_hline(aes(yintercept=10, linetype=esc("dotted")))
        p += legend_side
        p += facet_grid(".~sf~qtype", scales=esc("free_x"), space=esc("free_x"))
        postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
        ggsave("figures/tpch_{}.png".format(y_axis), p, postfix=postfix,  width=12, height=6, scale=0.8)
    
q = f"""
select lineage_type, sf, query,
avg(exec_roverhead), max(exec_roverhead), min(exec_roverhead),
avg(exec_overhead), max(exec_overhead), min(exec_overhead),
avg(mat_roverhead), max(mat_roverhead), min(mat_roverhead),
from tpch_metrics
where  lineage_type='Baseline' and sf=10
group by sf, lineage_type, query
order by sf, lineage_type, query
"""
out = con.execute(q).fetchdf()
print(out)
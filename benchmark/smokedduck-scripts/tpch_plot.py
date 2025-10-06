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

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--db', type=str, help='queries folder', default='tpch_benchmark_capture_may27_e.csv')
args = parser.parse_args()


con = duckdb.connect(args.db)
con.create_function("getMat", getMat, [VARCHAR], FLOAT)
con.create_function("getAllExec", getAllExec, [VARCHAR], FLOAT)
con.create_function("getTime", getTime, [VARCHAR], FLOAT)
con.create_function("cat", cat, [BIGINT], VARCHAR)
tpch_df = con.execute("""select *, cat(query) as qtype,
    getTime(plan) as plan_runtime,
    runtime - (getAllExec(plan_timings) - getMat(plan_timings)) mat_t ,
    getAllExec(plan_timings) - getMat(plan_timings) as exec_t,
    runtime as all_t
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
metrics = ["mat_t", "exec_t", "all_t", "runtime", "output", "plan_runtime", "lineage_size", "lineage_count", "nchunks", "postprocess_time"]
g = ','.join(header_unique)
m = ','.join(metrics)
avg_tpch = con.execute("""select {},
                            max(nchunks) as nchunks,
                            max(lineage_size) as lineage_size, max(lineage_count) as lineage_count,
                            avg(postprocess_time) as postprocess_time,
                            avg(plan_runtime) as plan_runtime, avg(runtime) as runtime,
                            avg(output) as output, 
                            
                            avg(all_t) as all_t, avg(exec_t) as exec_t, avg(mat_t) as mat_t
                            from tpch_all
                            group by {}""".format(g, g)).fetchdf()
header_unique.remove("lineage_type")
g = ','.join(header_unique)

tpch_withbaseline = con.execute(f"""select
                  t1.all_t as base_all_t, t1.exec_t as base_exec_t, t1.mat_t as base_mat_t,

                  t1.plan_runtime as base_plan_runtime, t1.runtime as base_runtime,
                  t1.output as base_output, 
                  t1.lineage_size as base_lineage_size,
                  t1.lineage_count as base_lineage_count,
                  t1.postprocess_time as base_pp,
                  t2.* from (select {g}, {m} from avg_tpch where lineage_type='Baseline') as t1
                  join avg_tpch  as t2 using ({g})
                  """).fetchdf()

tpch_metrics = con.execute("""
select {}, lineage_type, n_threads, output / base_output as fanout, output, nchunks, lineage_size, lineage_count, postprocess_time,

(exec_t-base_exec_t)*1000 as exec_overhead,
((exec_t-base_exec_t)/base_all_t)*100 as exec_roverhead,

(mat_t - base_mat_t)*1000 as mat_overhead,
((mat_t - base_mat_t) /base_all_t) *100 as mat_roverhead,

(all_t-base_all_t)*1000 as all_overhead,
((all_t-base_all_t)/base_all_t)*100 as all_roverhead,

(all_t-base_all_t)*1000 as overhead,
((all_t-base_all_t)/base_all_t)*100 as roverhead,

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
data = con.execute("select * from data where system<>'Baseline'").df()
sf = 1
data_sf = con.execute(f"select * from data where system<>'Baseline' and sf={sf} and n_threads=1").df()
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
        ggsave("figures/tpch_{}.png".format(y_axis), p, postfix=postfix,  width=14, height=6, scale=0.8)

        # TODO: plot sf=20
        p = ggplot(data_sf, aes(x='qid', ymin=0, ymax=y_axis,  y=y_axis, color='system', fill='system', group='system', shape='overheadType'))
        p += geom_point(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.5, size=2)
        p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.8)
        if y_axis == 'overhead':
            p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[10, 100, 1000], labels=list(map(esc, ['10', '100', '1000']))))
        else:
            p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[10, 100, 1000], labels=list(map(esc, ['10', '100', '1000']))))
            p += geom_hline(yintercept=20, linetype=esc("dotted"))
        p += legend_side
        p += facet_grid(".~qtype", scales=esc("free_x"), space=esc("free_x"))
        postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
        ggsave("figures/tpch_sample_{}.png".format(y_axis), p, postfix=postfix,  width=14, height=3.5, scale=0.8)
    
q = f"""
select lineage_type, sf, query,
avg(exec_roverhead) avg_eroverhead, max(exec_roverhead) max_eroverhead,
min(exec_roverhead) min_eroverhead,
avg(exec_overhead) avg_eoverhead, max(exec_overhead) max_eoverhead, 
avg(mat_roverhead) avg_mroverhead, max(mat_roverhead) max_mroverhead, 
avg(mat_overhead) avg_moverhead, max(mat_overhead) max_moverhead, 
from tpch_metrics
where  sf=10
group by sf, lineage_type, query
order by sf, lineage_type, query
"""
out = con.execute(q).fetchdf()
print(out)

# TODO summary per system per query per sf per thread
sf_list = [1, 10, 20]
th_list = con.execute("select distinct n_threads from tpch_metrics order by n_threads").df()
detailed_summary = True
if detailed_summary:
    for sf in sf_list:
        for sys in ["SD_Capture", "Logical-RID", "Logical-OPT", "Logical-window"]:
            for th in [1]: #th_list['n_threads']:
                print(f"=========== {sys} {sf} {th} ===============")
                q = f"""
            select lineage_type, sf, query, lineage_count, lineage_size, exec_roverhead, exec_overhead, mat_roverhead, mat_overhead, roverhead, overhead
            from tpch_metrics
            where  sf={sf} and lineage_type='{sys}' and n_threads={th}
            order by sf, lineage_type, query
                """
                out = con.execute(q).df()
                print(out)
                print("=================================")


q = f"""
select lineage_type, sf, n_threads, qtype,
avg(exec_roverhead) avg_eroverhead, max(exec_roverhead) max_eroverhead,
min(exec_roverhead) min_eroverhead,
avg(mat_roverhead) avg_mroverhead, max(mat_roverhead) max_mroverhead, 
avg(roverhead) avg_roverhead, max(roverhead) max_roverhead, 
from tpch_metrics where lineage_type<>'Baseline' and n_threads=1
group by sf, qtype, lineage_type, n_threads
order by sf, qtype, lineage_type, n_threads
"""
out = con.execute(q).df()
print(out)

for sf in sf_list:
    print(f"======== {sf} ==========")
    # TODO: measure the wins of applying optimizations on logical
    q = f"""select sf, query, sys.qtype, n_threads, sys.lineage_type, logical.roverhead/sys.roverhead as speedup
    from (select * from tpch_metrics where lineage_type='Logical-OPT') as logical JOIN
         (select * from tpch_metrics where lineage_type IN ('Logical-window', 'SD_Capture')) as sys
         USING (query, sf, n_threads)
         where sf={sf} and n_threads=1
         order by sys.lineage_type, sf, query, n_threads
         """
    speedup_df = con.execute(q).df()
    print(speedup_df)

    q = f"""select sf, qtype, n_threads, lineage_type, avg(speedup), max(speedup), min(speedup) from speedup_df
            group by sf, qtype, n_threads, lineage_type order by sf, qtype, lineage_type, n_threads
         """
    print(con.execute(q).df())

    q = f"""select sf, n_threads, lineage_type, avg(speedup), max(speedup), min(speedup) from speedup_df
            group by sf, n_threads, lineage_type order by sf, lineage_type, n_threads
         """
    print(con.execute(q).df())

print(f"======== lineage size summary ==========")
print(con.execute("""select sf, lineage_type, avg(lineage_size), max(lineage_size), min(lineage_size) from avg_tpch as t1
where n_threads=1
group by sf, lineage_type order by sf, lineage_type
    """).df())

print(con.execute("""select sf, t1.lineage_type, avg(t1.lineage_size/sd.lineage_size),
max(t1.lineage_size/sd.lineage_size),
min(t1.lineage_size/sd.lineage_size)
from avg_tpch as t1
join (select * from avg_tpch where lineage_type='SD_Capture' and lineage_size>0) as sd using (sf, query, n_threads)
where t1.n_threads=1
group by sf,  t1.lineage_type
    """).df())


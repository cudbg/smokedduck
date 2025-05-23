import json
import pandas as pd
import argparse
from pygg import *
import duckdb
from duckdb.typing import *
from utils import legend_bottom, legend_side, relative_overhead, overhead, getAllExec, getMat, get_op_timings

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--db', type=str,  help="exp notes", default="")
args = parser.parse_args()

dbname = args.db
con = duckdb.connect(dbname)

print(con.execute("pragma show_tables").df())
data = con.execute("select * from baselines").df() # where hash_agg_force='False'").df()
print(data)
print(data.columns.tolist())


con.create_function("getMat", getMat, [VARCHAR], FLOAT)
con.create_function("getAllExec", getAllExec, [VARCHAR], FLOAT)
con.create_function("get_op_timings", get_op_timings, [VARCHAR, VARCHAR], FLOAT)

metrics = ["runtime", "output_size", 'all_t', 'mat_t', 'exec_t']
m = ','.join(metrics)

header_unique = ["lineage_type", "sf", "hash_agg_force"]
g = ','.join(header_unique)

avg_metrics = """
max(output) as output_size,
avg(runtime) as runtime"""

avg_overhead = """
avg(getAllExec(plan_timings)) as all_t,
avg(getMat(plan_timings)) as mat_t,
avg(getAllExec(plan_timings) - getMat(plan_timings)) as exec_t
"""

# 1) average over iterations
data_avg = con.execute(f"""select {g}, {avg_metrics}, {avg_overhead}
                            from data
                            group by {g} order by {g}""").fetchdf()

aug_baseline = """
t1.all_t as base_all_t,
t1.runtime as base_runtime,
t1.output_size as base_output, 
t1.mat_t as base_mat_t,
t1.exec_t as base_exec_t,
t2.* 
"""
print(data_avg)
header_unique.remove("lineage_type")
g = ','.join(header_unique)
wbaseline = con.execute(f"""select {aug_baseline}
                  from (select {g}, {m} from data_avg where lineage_type='Baseline') as t1
                  join data_avg as t2 using ({g})
                  """).df()

print(wbaseline)

overheads = """
(exec_t-base_exec_t)*1000 as overhead,
((exec_t-base_exec_t)/base_exec_t)*100 as roverhead,

(mat_t - base_mat_t)*1000 as mat_overhead,
((mat_t - base_mat_t) / base_exec_t) *100 as mat_roverhead,

(all_t-base_all_t)*1000 as all_overhead,
((all_t-base_all_t)/base_exec_t)*100 as all_roverhead,
"""
mdata = con.execute(f"""select 'q1' as query, {g}, lineage_type, {m}, {overheads}
                  from wbaseline where lineage_type<>'Baseline'
                  """).df()
print(mdata)
# plot: (lineage_type) x-axis, (overhead/relative overhead) y-axis
y_axis_list = ["roverhead", "overhead"]
y_header = ["Relative\nOverhead %", "Overhead (ms)"]
linetype = "overheadtype"
for idx, y_axis in enumerate(y_axis_list):
    x_axis, x_label = "lineage_type", "System"
    x_type, y_type, y_label = "discrete",  "continueous", "{}".format(y_header[idx])
    fname, w, h = "baselines_{}.png".format(y_axis), 6, 3
    p = ggplot(mdata, aes(x=x_axis, y=y_axis, fill='query', group='query'))
    p += axis_labels(x_label, y_label, x_type, y_type)
    p += geom_bar(stat=esc('identity'), alpha=0.8, posiion=position_dodge(width=0.6), width=0.5)
    p += legend_bottom
    p += facet_grid(".~sf", scales=esc("free_x"), space=esc("free_x"))
    ggsave("figures/"+fname, p,  width=w, height=h, scale=0.8)

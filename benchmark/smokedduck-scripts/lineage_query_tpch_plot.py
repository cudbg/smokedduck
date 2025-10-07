import pandas as pd
from pygg import *
import duckdb

db = 'tpch_bw_seen.db'
#db = 'tpch_bw_all.db'
#db = 'tpch_bw_lineage.db'
con = duckdb.connect(db)

# ADD numbers for q16: 60ms

# Source Sans Pro Light
legend = theme_bw() + theme(**{
  "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
  "legend.justification":"c(1,0)", "legend.position":"c(1,0)",
  "legend.key" : element_blank(),
  "legend.title":element_blank(),
  "text": element_text(colour = "'#333333'", size=11, family = "'Arial'"),
  "axis.text": element_text(colour = "'#333333'", size=11),  
  "plot.background": element_blank(),
  "panel.border": element_rect(color=esc("#e0e0e0")),
  "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
  "strip.text": element_text(color=esc("#333333"))
  
})
# need to add the following to ggsave call:
#    libs=['grid']
legend_bottom = legend + theme(**{
  "legend.position":esc("bottom"),
  #"legend.spacing": "unit(-.5, 'cm')"

})

legend_side = legend + theme(**{
  "legend.position":esc("right"),
  "legend.margin":"margin(t = 0, unit='cm')"
})



data_comp = con.execute("""
    select t1.sf, t1.runtime*1000 as sd, t2.runtime*1000 as logical, t1.query::text as Query, t1.n as n_sd, t2.n as n_logical
    from (select sf, query, avg(runtime) as runtime, avg(output_size) as n from tpch_bw where lineage_type='SD' group by sf, query
    UNION ALL select 10 as sf, 16 as query, 0.001 as runtime, 10 as n) t1 
    JOIN (select sf, query, avg(runtime) as runtime, avg(output_size) as n  from tpch_bw where lineage_type='Logical-RID' group by sf, query) as t2
    USING (query, sf)
""").df()
print(data_comp)

data_sf10 = con.execute("select * from data_comp where sf=10").df()
p = ggplot(data_sf10, aes(x='Query', ymin='sd', ymax='logical'))  #, condition='system', color='system', fill='system', group='system')) \
p += geom_linerange(color=esc("gray")) 
p += geom_point(aes(y='sd', color=esc('SD'), shape=esc("SD")))
p += geom_point(aes(y='logical', color=esc('Logical'), shape=esc("Logical")))
p += scale_colour_discrete(name=esc("System"))
p += geom_hline(yintercept=1000, linetype=esc("dotted"))
p += geom_hline(yintercept=100, linetype=esc("dotted"))
p += scale_shape_discrete(name=esc("System"))
#p += facet_grid(".~sf")
p += axis_labels("TPC-H Query", "Runtime (log)", "continuous", "log10",
        ykwargs=dict(breaks=[1, 10, 100, 1000, 10000], labels=[esc('1ms'), esc('10ms'), esc('100ms'), esc('1s'), esc('10s')]))
p += legend_side
ggsave("figures/querying_tpch_sf1.png", p, width=4, height=1.8)

print(con.execute("select sf, lineage_type,  query, avg(runtime)*1000 as runtime from tpch_bw group by sf, query, lineage_type order by sf, query, lineage_type").df() )
print(con.execute("select sf, lineage_type,  avg(runtime)*1000, max(runtime)*1000, min(runtime)*1000 as runtime from tpch_bw group by lineage_type, sf").df() )

data_post = con.execute("""
with data as (
   select sf, query, 'PostProcess' as  ablation, avg(build_time) as avg_duration from tpch_bw where lineage_type='SD'
   group by sf, query, lineage_type UNION ALL
   select sf, query, 'Indexes' as  ablation, avg(runtime) as avg_duration from tpch_bw where lineage_type='SD' 
   group by sf, query, lineage_type UNION ALL
   select sf, query, 'Logical' as  ablation, avg(runtime) as avg_duration from tpch_bw where lineage_type='Logical'
   group by sf, query, lineage_type UNION ALL
   select sf, query, 'Q+' as  ablation, avg(base_runtime) as avg_duration from tpch_bw where lineage_type='SD'
   group by sf, query, lineage_type UNION ALL
   select sf, query, 'Q' as  ablation, avg(base_runtime) as avg_duration from tpch_bw where lineage_type='Logical'
   group by sf, query, lineage_type
) 
select query::text as query_id, sf, ablation, avg_duration * 1000 as avg_duration 
from data
""").fetchdf()
p = ggplot(data_post, aes(x='query_id', ymin=0, ymax='avg_duration', y='avg_duration', condition='ablation', color='ablation', fill='ablation', group='ablation', factor='avg_duration')) 
p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5) 
p += geom_point(aes(shape='ablation'), position=position_dodge(width=0.6), width=0.5, size=2) 
p += scale_y_log10(name=esc("Runtime (log)"), breaks=[1, 10, 100, 1000, 10000], labels=[esc('1ms'), esc('10ms'), esc('100ms'), esc('1s'), esc('10s')]) 
p += scale_x_discrete(name=esc("TPC-H Query"))
p += scale_color_discrete(name=esc("ablation") )
p += scale_fill_discrete(name=esc("ablation") )
p += facet_grid(".~sf")
p += legend_side
ggsave("figures/ablation.png", p, width=13, height=2)


print(data_sf10)
print(con.execute("""select lineage_type, sf, count() from
(select Query, sf, sd as runtime, 'sd' as lineage_type from data_comp 
UNION ALL select Query, sf, logical, 'logical' as lineage_type from data_comp) where runtime < 100 group by lineage_type, sf""").df())

print(con.execute("""select lineage_type, sf, count() from
(select Query, sf, sd as runtime, 'sd' as lineage_type from data_comp 
UNION ALL select Query, sf, logical, 'logical' as lineage_type from data_comp) where runtime > 100 and runtime < 1000 group by lineage_type, sf""").df())

print(con.execute("""select sf, Query, sd, logical, logical / sd as s from data_comp order by sf, s""").df())
print(con.execute("""select sf,  avg(sd), max(sd), avg(logical), max(logical), avg(logical / sd) as avg_s, max(logical/sd) max_s, min(logical/sd) min_s
from data_comp group by sf, order by sf""").df())

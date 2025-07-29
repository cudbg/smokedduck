import pandas as pd
from pygg import *
import duckdb

db = 'tpch_bw_test.db'
con = duckdb.connect(db)

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



data = con.execute("""
    select t1.sf, t1.runtime*1000 as sd, t2.runtime*1000 as logical, t1.query::text as Query
    from (select sf, query, avg(runtime) as runtime from tpch_bw where lineage_type='SD' group by sf, query) t1 
    JOIN (select sf, query, avg(runtime) as runtime  from tpch_bw where lineage_type='Logical-RID' group by sf, query) as t2
    USING (query, sf)

""").df()
print(data)

p = ggplot(data, aes(x='Query', ymin='sd', ymax='logical'))  #, condition='system', color='system', fill='system', group='system')) \
p += geom_linerange(color=esc("gray")) 
p += geom_point(aes(y='sd', color=esc('SD'), shape=esc("SD")))
p += geom_point(aes(y='logical', color=esc('Logical'), shape=esc("Logical")))
p += scale_colour_discrete(name=esc("System"))
p += geom_hline(yintercept=500, linetype=esc("dotted"))
p += geom_hline(yintercept=100, linetype=esc("dotted"))
p += scale_shape_discrete(name=esc("System"))
p += facet_grid(".~sf")
p += axis_labels("TPC-H Query", "Runtime (log)", "continuous", "log10",
        ykwargs=dict(breaks=[1, 10, 100, 500, 2000, 10000], labels=[esc('1ms'), esc('10ms'), esc('100ms'), esc('500ms'), esc('2s'), esc('10s')]))
p += legend_side
ggsave("querying_tpch_sf1.png", p, width=5, height=2)

print(con.execute("select sf, lineage_type,  query, avg(runtime)*1000 as runtime from tpch_bw group by sf, query, lineage_type order by sf, query, lineage_type").df() )
print(con.execute("select sf, lineage_type,  avg(runtime)*1000, max(runtime)*1000, min(runtime)*1000 as runtime from tpch_bw group by lineage_type, sf").df() )

data = con.execute("""
with data as (
   select sf, query, 'PostProcess' as  ablation, avg(build_time) as avg_duration from tpch_bw where lineage_type='SD'
   group by sf, query, lineage_type UNION ALL
   select sf, query, 'Indexes' as  ablation, avg(runtime) as avg_duration from tpch_bw where lineage_type='SD' 
   group by sf, query, lineage_type UNION ALL
   select sf, query, 'Q' as  ablation, avg(base_runtime) as avg_duration from tpch_bw where lineage_type='SD'
   group by sf, query, lineage_type
) 
select query::text as query_id, sf, ablation, avg_duration * 1000 as avg_duration 
from data
""").fetchdf()
p = ggplot(data, aes(x='query_id', ymin=0, ymax='avg_duration', y='avg_duration', condition='ablation', color='ablation', fill='ablation', group='ablation', factor='avg_duration')) 
p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5) 
p += geom_point(aes(shape='ablation'), position=position_dodge(width=0.6), width=0.5, size=2) 
p += scale_y_log10(name=esc("Runtime (log)"), breaks=[1, 10, 100, 1000, 10000], labels=[esc('1ms'), esc('10ms'), esc('100ms'), esc('1s'), esc('10s')]) 
p += scale_x_discrete(name=esc("TPC-H Query"))
p += scale_color_discrete(name=esc("ablation") )
p += scale_fill_discrete(name=esc("ablation") )
p += facet_grid(".~sf")
p += legend_side
ggsave("ablation.png", p, width=13, height=2)



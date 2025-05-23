#!/bin/bash

current_date=$(date +"%Y%m%d_%H%M")
note="exp_$current_date"
db="baselines_$note.db"
repeat=3
sf_list=("1" "10" "20")
mkdir figures

for sf in ${sf_list[@]}
do
  python3 ~/smokedduck/benchmark/smokedduck-scripts/baselines.py --notes $note --repeat $repeat --save --sf $sf
  python3 ~/smokedduck/benchmark/smokedduck-scripts/baselines.py --notes $note --repeat $repeat --save --sf $sf --hg
  python3 ~/smokedduck/benchmark/smokedduck-scripts/baselines.py --notes $note --repeat $repeat --save --sf $sf --smoke
  python3 ~/smokedduck/benchmark/smokedduck-scripts/baselines.py --notes $note --repeat $repeat --save --sf $sf --smoke --hg
  python3 ~/smokedduck/benchmark/smokedduck-scripts/baselines.py --notes $note --repeat $repeat --save --sf $sf --lineage
  python3 ~/smokedduck/benchmark/smokedduck-scripts/baselines.py --notes $note --repeat $repeat --save --sf $sf --lineage --hg
  python3 ~/smokedduck/benchmark/smokedduck-scripts/baselines.py --notes $note --repeat $repeat --save --sf $sf --perm
  python3 ~/smokedduck/benchmark/smokedduck-scripts/baselines.py --notes $note --repeat $repeat --save --sf $sf --perm --hg
done


python3 ~/smokedduck/benchmark/smokedduck-scripts/baselines_plot.py --db $db 

echo $note

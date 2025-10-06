#!/bin/bash

# exp_20250920_1749
current_date=$(date +"%Y%m%d_%H%M")
#current_date=$(date +"%Y%m%d")
note="exp_$current_date"
sf_list=("1" "10") # "10" "20")
repeat=4
mat= #--mat"

mkdir figures

for sf in ${sf_list[@]}
do
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf $mat
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf $mat --normalized
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --lineage $mat
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --lineage --normalized $mat
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --perm $mat
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --perm --op $mat
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --gprom $mat
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --logical_list $mat
done

python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_plot.py --db tpch_benchmark_capture_$note.db

echo $note

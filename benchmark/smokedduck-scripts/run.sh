#!/bin/bash

current_date=$(date +"%Y%m%d_%H%M")
#current_date=$(date +"%Y%m%d")
note="exp_$current_date"
sf_list=("1" "10") # "10" "20")
repeat=3
mkdir figures

for sf in ${sf_list[@]}
do
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --lineage
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --perm
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --perm --opt
  python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_capture.py $note --repeat $repeat --save_csv --csv_append --sf $sf --gprom
done

python3 ~/smokedduck/benchmark/smokedduck-scripts/tpch_plot.py --db tpch_benchmark_capture_$note.db

echo $note

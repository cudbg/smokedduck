#!/bin/bash

current_date=$(date +"%Y%m%d_%H%M")
#current_date=$(date +"%Y%m%d")
note="exp_$current_date"
db="micro_benchmark_$note.out"
repeat=3
mkdir figures

#rm filter_micro_db.out
#rm join_micro_db.out
#rm micro_agg_db.out
#rm micro_agg_db_v2.out
exps="--run_filter" # --run_agg --run_hj --run_hj_mtn" # micro_benchmark_exp_20250526_2235.out
exps="--run_agg" 
exps="--run_hj" 

linux_folder=~/smokedduck/benchmark/smokedduck-scripts/
mac_folder=benchmark/smokedduck-scripts/micro_benchmark/

folder=$mac_folder

# add option to do mat and no mat
mat="" #"--mat"

# TODO: add dry run

# iterate over each experiment
python3 $folder/micro_run.py --notes $note --repeat $repeat --save $exps $mat
python3 $folder/micro_run.py --notes $note --repeat $repeat  --save --lineage $exps $mat
python3 $folder/micro_run.py --notes $note --repeat $repeat --save $exps --smoke $mat
python3 $folder/micro_run.py --notes $note --repeat $repeat --save --perm $exps $mat

## only for lineage extension
#python3 $folder/micro_run.py --notes $note --repeat $repeat --save $exps --lineage --op_level $mat
#python3 $folder/micro_run.py --notes $note --repeat $repeat --save $exps --lineage --op_level --no_persist $mat

## only for extension with hybrid support
#python3 $folder/micro_run.py --notes $note --repeat $repeat --save $exps --lineage --hybrid $mat
#python3 $folder/micro_run.py --notes $note --repeat $repeat --save $exps --lineage --hybrid --no_persist $mat

python3 $folder/micro_plot.py --db $db $exps

echo $note

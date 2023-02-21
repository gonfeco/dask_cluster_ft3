#!/bin/bash
echo SLURM_NTASKS: $SLURM_NTASKS  
echo SLURM_PROCID: $SLURM_PROCID

TASKFORPYTHON=$((SLURM_NTASKS-1))

#SCHED_FILE=./zalo.json
#echo $SCHED_FILE

if [ $SLURM_PROCID == $TASKFORPYTHON ];
then
    echo "EXECUTING YOUR PYTHON PROGRAM"
    #Tiempo para que el cluster de dask se monte antes de empezar a lanzar calculos
    sleep 5
    python ./inc_dask.py 
else
    echo "RAISING THE DASK CLUSTER"
    python ./dask_cluster.py \
        -local $LUSTRE_SCRATCH \
        --dask_cluster \
        --ib \
        #-scheduler_file $SCHED_FILE \
        #-preload  ./PreLoad.py

fi

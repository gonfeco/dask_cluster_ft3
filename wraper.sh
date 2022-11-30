#!/bin/bash
echo SLURM_NTASKS: $SLURM_NTASKS  
echo SLURM_PROCID: $SLURM_PROCID

TASKFORPYTHON=$((SLURM_NTASKS-1))

SCHED_FILE=/home/cesga/gferro/dfailde/dask_cluster/zalo.json
echo $SCHED_FILE

if [ $SLURM_PROCID == $TASKFORPYTHON ];
then
    echo "EXECUTING YOUR PYTHON PROGRAM"
    #Para que monte el cluster antes de lanzar nada
    sleep 2
    python ./inc_dask.py 
else
    echo "RAISING THE DASK CLUSTER"
    python ./dask_cluster.py -local $LUSTRE_SCRATCH --dask_cluster #-scheduler_file $SCHED_FILE
fi

#!/bin/bash
echo SLURM_NTASKS: $SLURM_NTASKS  
echo SLURM_PROCID: $SLURM_PROCID

TASKFORPYTHON=$((SLURM_NTASKS-1))

if [ $SLURM_PROCID == $TASKFORPYTHON ];
then
    echo "EXECUTING YOUR PYTHON PROGRAM"
    python ./inc_dask.py 
else
    echo "RAISING THE DASK CLUSTER"
    python ./dask_cluster_resvports.py -local $LUSTRE_SCRATCH --dask_cluster 
fi

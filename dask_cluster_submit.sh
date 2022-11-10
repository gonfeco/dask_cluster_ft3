#!/bin/bash

#SBATCH -n 4 # Number of tasks 
#SBATCH -c 2 # Total number of core for one task
#SBATCH --mem-per-cpu=3G
#SBATCH -t 00:10:00

# SBATCH --ntasks-per-node=4

# Number of tasks 
echo SLURM_NTASKS: $SLURM_NTASKS  
echo SLURM_NTASKS_PER_NODE: $SLURM_NTASKS_PER_NODE
echo SLURM_CPUS_PER_TASK: $SLURM_CPUS_PER_TASK 
echo SLURM_NNODES: $SLURM_NNODES
echo SLURM_MEM_PER_CPU: $SLURM_MEM_PER_CPU

########## MODULE LOADING ############
module load miniconda3
conda activate qiskit_dask
#####################################




IB="IB"
#For No IB use:
#IB="NoIB"

################## GETTING IP OF ALL NODES OF THE JOB ##########################
PATHTOPACKAGE="./"
export TFSERVER=""
if [ $IB = "NoIB" ]
then
    #Get the IPs of all nodes of the allocated job. Ethernet IPs
    TFSERVER=$(srun -n $SLURM_NNODES --ntasks-per-node=1 $PATHTOPACKAGE/Wraper_NoIB.sh)
    else
    #Get the IPs of all nodes of the allocated job. Infiny Band IPs
    TFSERVER=$(srun -n $SLURM_NNODES --ntasks-per-node=1 $PATHTOPACKAGE/Wraper_IB.sh)
fi
################################################################################
sleep 2



srun -n $SLURM_NTASKS -c $SLURM_CPUS_PER_TASK --mem-per-cpu $SLURM_MEM_PER_CPU -l \
    python ./dask_cluster.py -local $LUSTRE_SCRATCH

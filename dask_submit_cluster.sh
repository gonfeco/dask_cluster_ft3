#!/bin/bash

#SBATCH -n 8 # Number of tasks 
#SBATCH -c 4 # Total number of core for one task
#SBATCH --mem-per-cpu=3G
# SBATCH -C clk #For submit to clk
#SBATCH -t 00:15:00

MEMORY_PER_TASK=$(( $SLURM_CPUS_PER_TASK*$SLURM_MEM_PER_CPU ))

# Number of tasks 
echo SLURM_NTASKS: $SLURM_NTASKS  
echo SLURM_NTASKS_PER_NODE: $SLURM_NTASKS_PER_NODE
echo SLURM_CPUS_PER_TASK: $SLURM_CPUS_PER_TASK 
echo SLURM_NNODES: $SLURM_NNODES
echo SLURM_MEM_PER_CPU: $SLURM_MEM_PER_CPU
echo MEMORY_PER_TASK: $MEMORY_PER_TASK

########## MODULE LOADING ############
#module load cesga/2020 gcc/system openmpi/4.0.5_ft3 dask/2021.3.0-python-3.6.12 #-> WORKS

# module load cesga/2020 gcc/system openmpi/4.0.5_ft3 dask/2021.6.0 #-> WORKS

module load cesga/2020 gcc/system openmpi/4.0.5_ft3_cuda dask/2022.2.0

#module load miniconda3
#conda activate qiskit_dask
#####################################

rm -f scheduler_info.txt
rm -f ssh_command.txt


#SCHED_FILE="./zalo.json"

#--mem-per-cpu=$SLURM_MEM_PER_CPU \
#Reserved Ports Version
srun -n $SLURM_NTASKS \
    -c $SLURM_CPUS_PER_TASK \
	--resv-ports=$SLURM_NTASKS -l \
    --mem=$MEMORY_PER_TASK \
    python ./dask_cluster.py \
        -local $LUSTRE_SCRATCH \
        --dask_cluster \
        --ib \
        #-scheduler_file $SCHED_FILE \
        #-preload  ./PreLoad.py

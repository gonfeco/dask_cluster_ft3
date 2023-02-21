#!/bin/bash

#SBATCH -n 6 # Number of tasks 
#SBATCH -c 1 # Total number of core for one task
# SBATCH -C clk #For submit to clk
#SBATCH --mem-per-cpu=3G
#SBATCH -t 00:10:00

# SBATCH --ntasks-per-node=4

MEMORY_PER_TASK=$(( $SLURM_CPUS_PER_TASK*$SLURM_MEM_PER_CPU ))
# Number of tasks 
echo SLURM_NTASKS: $SLURM_NTASKS  
echo SLURM_NTASKS_PER_NODE: $SLURM_NTASKS_PER_NODE
echo SLURM_CPUS_PER_TASK: $SLURM_CPUS_PER_TASK 
echo SLURM_NNODES: $SLURM_NNODES
echo SLURM_MEM_PER_CPU: $SLURM_MEM_PER_CPU

########## MODULE LOADING ############
#module load cesga/2020 gcc/system openmpi/4.0.5_ft3 dask/2021.3.0-python-3.6.12 #-> WORKS

# module load cesga/2020 gcc/system openmpi/4.0.5_ft3 dask/2021.6.0 #-> WORKS

module load cesga/2020 gcc/system openmpi/4.0.5_ft3_cuda dask/2022.2.0

#module load miniconda3
#conda activate qiskit_dask
#####################################

rm -f scheduler_info.txt
rm -f ssh_command.txt


TASKS_FOR_CLUSTER=$((SLURM_NTASKS-1))
echo 'TASKS_FOR_CLUSTER= '$TASKS_FOR_CLUSTER

#srun -n $SLURM_NTASKS -c $SLURM_CPUS_PER_TASK \
#    --mem-per-cpu $SLURM_MEM_PER_CPU -l \
#    python ./dask_cluster.py -local $LUSTRE_SCRATCH --dask_cluster

#srun -n 1 -c $SLURM_CPUS_PER_TASK \
#    --mem-per-cpu $SLURM_MEM_PER_CPU \
#    python ./inc_dask.py &

#--mem-per-cpu $SLURM_MEM_PER_CPU \
#Reserved Ports Version
srun -n $SLURM_NTASKS \
    -c $SLURM_CPUS_PER_TASK \
    --mem=$MEMORY_PER_TASK \
	--resv-ports=$TASKS_FOR_CLUSTER -l \
    ./wraper.sh
    #python ./dask_cluster_resvports.py -local $LUSTRE_SCRATCH --dask_cluster 



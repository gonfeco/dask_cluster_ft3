#!/bin/bash

#SBATCH -n 12 # Number of tasks 
#SBATCH -c 2 # Total number of core for one task
#SBATCH --mem-per-cpu=3G
# SBATCH --gres=gpu:a100:2 
#SBATCH -p hwl
# SBATCH --exclusive
#SBATCH -t 04:00:00



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

# module load cesga/2020 gcc/system openmpi/4.0.5_ft3_cuda dask/2022.2.0

#module load cesga/2020 gcc/system openmpi/4.0.5_ft3_cuda dask/2022.2.0
#module load cesga/2022 gcc/system myqlm/1.9.9
#module load cesga/2022 gcc/system openmpi/4.1.4 dask/2022.2.0 myqlm/1.9.9

module load cesga/system miniconda3/22.11.1-1
conda activate fibratic_ml_online

#module load miniconda3
#conda activate qiskit_dask
#####################################

rm -f scheduler_info.json
rm -f ssh_command.txt
#export DASK_CONFIG="/home/cesga/gferro/.config/dask/"


#SCHED_FILE="./zalo.json"

#--mem-per-cpu=$SLURM_MEM_PER_CPU \
#Reserved Ports Version
srun -n $SLURM_NTASKS \
    -c $SLURM_CPUS_PER_TASK \
    --mem=$MEMORY_PER_TASK \
    python ./dask_cluster.py \
        --dask_cluster \
        -local $LUSTRE_SCRATCH \
        --ib \
        -preload  ./PreLoad.py

    #--ib -preload  ./PreLoad.py
        #-scheduler_file $SCHED_FILE \
	#--resv-ports=$SLURM_NTASKS -l \
        #-local $LUSTRE_SCRATCH \
     #--resv-ports=$SLURM_NTASKS -l \

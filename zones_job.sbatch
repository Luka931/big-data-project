#!/bin/bash
#SBATCH --job-name=zones_final
#SBATCH --output=zones_final.log
#SBATCH --error=zones_final.log

#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=02:00:00

module load Anaconda3
eval "$(conda shell.bash hook)"
conda activate bd311

python ~/projects/big-data-project/zones_analysis.py

#! /bin/bash
export CHPL_LAUNCHER_USE_SBATCH=true
export SBATCH_TIMELIMIT=720

TRIALS=$1
TYPE=$2

for size in 16 17 18 19 20 21 22 23 24 25 26 27 28; do # input sizes
  for nl in 02 04 08 16 32; do # number of locales
    (set -x; \
      CHPL_LAUNCHER_SLURM_OUTPUT_FILENAME=rmat_${TYPE}_scale$size.nl$nl.out \
      ../BreadthFirstSearchBenchmarker -nl $nl --scale=$size --trials=$TRIALS --bfsAlgorithm=$TYPE --skipBFSprints=true)
  done
done

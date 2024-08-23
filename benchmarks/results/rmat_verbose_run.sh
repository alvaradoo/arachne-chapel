#! /bin/bash
export CHPL_LAUNCHER_USE_SBATCH=true
export SBATCH_TIMELIMIT=720

TRIALS=$1
TYPE=$2

for size in 16 17 18 19 20; do # input sizes
  for nl in 02 04 08; do # number of locales
    (set -x; \
      CHPL_LAUNCHER_SLURM_OUTPUT_FILENAME=rmat_${TYPE}_scale$size.nl$nl.comm.out \
      ../BreadthFirstSearchBenchmarker -nl $nl --scale=$size --trials=$TRIALS --bfsAlgorithm=$TYPE --measureComms=true --measureVerboseComms=true --skipBFSprints=true > rmat_${TYPE}_scale$size.nl$nl.dump)
  done
done

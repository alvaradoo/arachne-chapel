# Arachne-Chapel
Experimental harness for Arachne without Arkouda dependencies. To be used for benchmarking and optimizing graph algorithms in Chapel.

## Requirements
1. Every Chapel dependency.
2. Chapel itself. At the time of writing, the Chapel version utilized was `2.1.0`.

## Installation Instructions
Contained is a Makefile with the following rules: `test`, `benchmark`, and `benchmark_clean`. **To use any of this code within your own Chapel programs please add the `src/` directory to the `CHPL_MODULE_PATH` environment variable.** This can be done by executing:
```bash
export CHPL_MODULE_PATH=$CHPL_MODULE_PATH:/path/to/arachne-chapel/src
```

Then, you can write any Chapel program, for example we make `foo.chpl` below:
```chpl
// Chapel standard modules.
use BlockDist;

// Arachne modules.
use EdgeList;
use VertexCentricGraph;
use BreadthFirstSearch;

var src = blockDist.createArray({0..17}, int);
var dst = blockDist.createArray({0..17}, int);

src = [0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 9, 9, 10, 10, 10, 9];
dst = [0, 2, 3, 4, 4, 5, 9, 6, 7, 7, 8, 9, 9, 9, 11, 12, 15, 10];

var eGraph = new shared EdgeList(src, dst);
var vGraph = new shared VertexCentricGraph(eGraph);

var bfsResults = bfsParentVertexAgg(toGraph(vertexView), 0);
```

Then, you can compile it and run it with the following commands:
```chpl
chpl foo.chpl --fast
./foo -nl 2
```

**Please note:** Any graph kernel needs the `VertexCentricGraph` object to be casted to its `Graph` parent type. This is because a `Graph` object is a concrete, non-generic object that is needed for the graph kernel to be considered a first-class function (FCF). The graph kernels are required to be FCFs for the benchmark harness to function as intended. `VertexCentricGraph` objects are intended to be generic to allow for flexibility on the class variable types. For more information on FCFs and generics please consult the Chapel documentation:
1. FCFs: https://chapel-lang.org/docs/technotes/firstClassProcedures.html
2. Generics: https://chapel-lang.org/docs/language/spec/generics.html

### Building the Benchmark Harness
To compile all of the benchmarks found within the `benchmarks/` directory please execute:
```bash
make benchmark
```

To clean up that repository please execute:
```bash
make benchmark_clean
```

To execute any of the programs within `benchmarks/` please navigate to that repository and run any of the executables which should be named exactly as its Chapel file without the extension `.chpl` and without `_real`. Any results will be stored within the `benchmarks/results/` directory. 

#### Running the Benchmarks with Slurm
Chapel executables can be run directly with Slurm by specifyfing the environment variables `CHPL_LAUNCHER_USE_SBATCH=true` and `CHPL_LAUNCHER=slurm-srun` for `sbatch` and `srun` respectively. For more information on launchers please visit: https://chapel-lang.org/docs/usingchapel/launcher.html#currently-supported-launchers. 

For long-running tests it is recommended to use `sbatch`. There are two files contained within `/benchmarks/results/` named `rmat_run.sh` and `rmat_verbose_run.sh` that can be used as samples to set up your own benchmark scripts. These in their default set-up execute `BreadthFirstSearchBenchmarker` which is the executable generated from compiling `BreadthFirstSearchBenchmarker.chpl`. The former only outputs execution times whereas the latter saves communication information between locales as well as verbose outputs that can be parsed out with the script below to get the lines of code that performed the most communications.
```bash
# filter verbose comm output
filter_comm() {
  echo "remote get"
  grep "remote get" $1 | cut -d":" -f2,3 | sort | uniq -c | sort -rn
  echo "remote put"
  grep "remote put" $1 | cut -d":" -f2,3 | sort | uniq -c | sort -rn
  echo "remote executeOn"
  grep "remote executeOn" $1 | cut -d":" -f2,3 | sort | uniq -c | sort -rn
  echo "remote non-blocking executeOn"
  grep "remote non-blocking executeOn" $1 | cut -d":" -f2,3 | sort | uniq -c | sort -rn
  echo "remote fast executeOn"
  grep "remote fast executeOn" $1 | cut -d":" -f2,3 | sort | uniq -c | sort -rn
}
```
**Please inspect each individual benchmark file for command-line arguments that can be modified for specific functionality.**

### Executing Correctness Tests
The correctness tests within `tests/` rely on Chapel's testing harness to execute. To run all of the correctness tests please execute:
```bash
make test
```
Any tests you add to the `tests/` directory should automatically be executed as long as they have an attributed `*.good` file and begin with the word `test`. Further, any tests you write will be executed on a single locale unless you specify a `*.numlocales` file for that test. For more information on the Chapel Testing System please visit: https://chapel-lang.org/docs/developer/bestPractices/TestSystem.html.

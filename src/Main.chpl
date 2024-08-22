module Main {
  use Time;
  use Random;
  use CommDiagnostics;
  use IO.FormattedIO;
  use IO;
  use Search;
  use Sort;

  use Graph;
  use Generator;
  use VertexCentricGraph;
  use EdgeCentricGraph;
  use BreadthFirstSearch;
  use Utils;

  config const filepath:string;

  config const scale = 10;
  config const edgeFactor = 16;
  config const (a,b,c,d) = (0.57,0.19,0.19,0.05);
  config const maxWeight = 2;
  config const trials = 64;

  config const measureVerboseComms = false; // prints verbose comms
  config const measureComms = false; // sends verbose comms to table
  config const identifier = "rmat";
  config const bfsAlgorithm = "parentVertexAgg";

  config const skipValidation = true;
  config const skipBFSprints = false;

  var commFileIdentifier:string;

  /*
    The BFS procedures should be designed as first-class functions that 
    contain only non-generic parameters and do not return any generic values. 

    The parameter `methodReturn` is to specify if the BFS method returns a 
    parent or level/depth array to ensure the proper validation method is 
    utilized.
  */
  proc runBFS(method, graph, const ref sources, ref runs, ref teps, ref eCounts,
              methodReturn:string) {
    var timer:stopwatch;
    var methodFull = (method:string).split("(");
    var methodName:string;
    for (d,m) in zip(methodFull.domain, methodFull) {
      if d == 0 then methodName = m.replace("proc ", "");
      break;
    }

    for i in 1..trials {
      var source = sources[i];
      if !skipBFSprints then writef("Running %s %i\n", methodName, source);
      
      if measureVerboseComms then startVerboseComm();
      if measureComms then startCommDiagnostics();
      timer.start();
      var res = method(graph:shared Graph, source);
      timer.stop();
      if measureVerboseComms then stopVerboseComm();
      if measureComms then stopCommDiagnostics();

      var edgeCount = getEdgeCountForTeps(res, graph) / 2;
      /* Not needed yet until validation can be enabled.
      var levels = if methodReturn == "parent" then parentToLevel(res, source) 
                   else res; */
      runs[i] = timer.elapsed();
      eCounts[i] = edgeCount;
      teps[i] = eCounts[i] / runs[i];
      if !skipBFSprints {
        writef("Time for BFS %i is %dr\n", source, runs[i]);
        writef("TEPs for BFS %i is %r\n", source, teps[i]);
      }
      if !skipValidation then halt("Validation not yet integrated.");
      timer.reset();
    }
    if measureComms { 
      try! commDiagnosticsToCsv(getCommDiagnostics(), commFileIdentifier, methodName);
      resetCommDiagnostics();
    }
  }

  /*
    Computes statistics for a given data array which could be an array of
    execution times or TEPs for BFS or SSSP.
  */
  proc computeStatistics(in data) {
    // Sample size.
    var n = data.size;

    // Compute mean.
    var mean:real = + reduce data / n;
    
    // Compute standard deviation.
    var numerator:real = 0.0;
    for d in data do numerator += (d - mean)**2;
    var stdDev = sqrt(numerator/(n-1));

    // Sort data and then get order statistics.
    sort(data);
    var minimum = data[1];
    var firstQuartile = (data[n/4] + data[(n+1)/4]) * 0.5;
    var median = (data[n/2] + data[(n+1)/2]) * 0.5;
    var thirdQuartile = (data[n-(n/4)] + data[n-((n+1)/4)]) * 0.5;
    var maximum = data[n];

    return (mean,stdDev,minimum,firstQuartile,median,thirdQuartile,maximum);
  }

  proc main() {
    if a + b + c + d != 1 then
      halt("Kronecker parameters (a,b,c,d) do not add up to 1.");
    
    if filepath.size > 0 && identifier == "rmat" && measureComms then
      halt("When measureComms is true for a .mtx file, identifier must not be rmat.");

    var timer:stopwatch;
    var isRandom = if filepath.size > 0 then false else true;
    if measureComms && isRandom then commFileIdentifier = "benchmarks/" + identifier + "_" + scale:string;
    else if measureComms then commFileIdentifier = "benchmarks/" + identifier;
    else commFileIdentifier = "";

    var ns = if isRandom then 2**scale else 0;
    var ms = if isRandom then 2**scale * edgeFactor else 0;

    timer.start(); if measureComms then startCommDiagnostics();
    var edgeView: shared EdgeCentricGraph(?);
    if isRandom then edgeView = genRMATgraph(a,b,c,d,scale,ns,ms,maxWeight);
    else { try! edgeView = matrixMarketFileToGraph(filepath); }
    timer.stop(); if measureComms then stopCommDiagnostics();
    var edgeListGenTime = timer.elapsed();
    if measureComms {
      try! commDiagnosticsToCsv(getCommDiagnostics(), commFileIdentifier, "edgeListGen");
      resetCommDiagnostics();
    }
    timer.reset();

    timer.start(); if measureComms then startCommDiagnostics();
    var vertexView = new shared VertexCentricGraph(edgeView);
    timer.stop(); if measureComms then stopCommDiagnostics();
    var graphConstructionTime = timer.elapsed();
    if measureComms { 
      try! commDiagnosticsToCsv(getCommDiagnostics(), commFileIdentifier, "graphConstruction");
      resetCommDiagnostics();
    }
    timer.reset();

    var n = vertexView.numVertices;
    var m = vertexView.numEdges;

    var runs:[1..trials] real;
    var sources:[1..trials] int;
    var teps: [1..trials] real;
    var eCounts: [1..trials] int;
    fillRandom(sources, 0, n-1);
    
    if bfsAlgorithm == "graph500" then
      runBFS(bfsParentVertexGraph500, vertexView, sources, runs, teps, eCounts, "parent");
    else if bfsAlgorithm == "parentVertexAgg" then
      runBFS(bfsParentVertexAgg, vertexView, sources, runs, teps, eCounts, "parent");
    else if bfsAlgorithm == "jenkins" then
      runBFS(bfsParentVertexJenkins, vertexView, sources, runs, teps, eCounts, "parent");
    else if (bfsAlgorithm == "levelVertexAgg") then
      runBFS(bfsLevelVertexAgg, vertexView, sources, runs, teps, eCounts, "level");
    else if (bfsAlgorithm == "levelVertex") then
      runBFS(bfsLevelVertex, vertexView, sources, runs, teps, eCounts, "level");
    else if (bfsAlgorithm == "levelAggregationVertex") then
      runBFS(bfsAggregationVertex, vertexView, sources, runs, teps, eCounts, "level");
    else if (bfsAlgorithm == "levelNoAggregationVertex") then
      runBFS(bfsNoAggregationVertex, vertexView, sources, runs, teps, eCounts, "level");
    else halt("Unrecognized BFS method");

    writef("%<40s %i\n", "SCALE:", scale);
    writef("%<40s %i\n", "edgefactor:", edgeFactor);
    writef("%<40s %i\n", "NBFS:", trials);
    writef("%<40s %dr\n", "graph_generation:", edgeListGenTime);
    var taskCount:int = 0;
    coforall loc in Locales with (+ reduce taskCount) do on loc {
      taskCount += here.maxTaskPar;
    }
    writef("%<40s %i\n", "num chapel tasks:", taskCount);
    writef("%<40s %dr\n", "construction_time:", graphConstructionTime);
    
    var (bfsMean, bfsStdDev, bfsMinimum, bfsFirstQuartile, bfsMedian, 
         bfsThirdQuartile, bfsMaximum) = computeStatistics(runs);
    writef("%<40s %dr\n", "bfs  min_time:", bfsMinimum);
    writef("%<40s %dr\n", "bfs  firstquartile_time:", bfsFirstQuartile);
    writef("%<40s %dr\n", "bfs  median_time:", bfsMedian);
    writef("%<40s %dr\n", "bfs  thirdquartile_time:", bfsThirdQuartile);
    writef("%<40s %dr\n", "bfs  max_time:", bfsMaximum);
    writef("%<40s %dr\n", "bfs  mean_time:", bfsMean);
    writef("%<40s %dr\n", "bfs  stddev_time:", bfsStdDev);

    var (eMean, eStdDev, eMinimum, eFirstQuartile, eMedian, 
         eThirdQuartile, eMaximum) = computeStatistics(eCounts);
    writef("%<40s %i\n", "min_nedge:", eMinimum);
    writef("%<40s %i\n", "firstquartile_nedge:", eFirstQuartile);
    writef("%<40s %i\n", "median_nedge:", eMedian);
    writef("%<40s %i\n", "thirdquartile_nedge:", eThirdQuartile);
    writef("%<40s %i\n", "max_nedge:", eMaximum);
    writef("%<40s %i\n", "mean_nedge:", eMean);
    writef("%<40s %i\n", "stddev_nedge:", eStdDev);

    var (tepsMean, tepsStdDev, tepsMinimum, tepsFirstQuartile, tepsMedian, 
         tepsThirdQuartile, tepsMaximum) = computeStatistics(teps);
    writef("%<40s %r\n", "min_teps:", tepsMinimum);
    writef("%<40s %r\n", "firstquartile_teps:", tepsFirstQuartile);
    writef("%<40s %r\n", "median_teps:", tepsMedian);
    writef("%<40s %r\n", "thirdquartile_teps:", tepsThirdQuartile);
    writef("%<40s %r\n", "max_teps:", tepsMaximum);
    writef("%<40s %r\n", "harmonic_mean_teps:", tepsMean);

    // Below is originally from the Graph500 source code: 
    // https://github.com/graph500/graph500/tree/newreference
    /* Formula from:
    * Title: The Standard Errors of the Geometric and Harmonic Means and
    *        Their Application to Index Numbers
    * Author(s): Nilan Norris
    * Source: The Annals of Mathematical Statistics, Vol. 11, No. 4 (Dec., 1940), pp. 445-448
    * Publisher(s): Institute of Mathematical Statistics
    * Stable URL: http://www.jstor.org/stable/2235723
    * (same source as in specification). */
    var harmonicStdDevTeps = tepsStdDev/(tepsMean*tepsMean*sqrt(trials-1));
    writef("%<40s %r\n", "harmonic_stddev_teps:", harmonicStdDevTeps);
  }
}

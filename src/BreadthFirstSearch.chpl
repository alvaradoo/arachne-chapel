module BreadthFirstSearch {
  // Chapel modules.
  use List;
  use Set;
  use Time;
  use BlockDist;
  use ReplicatedDist;
  use ReplicatedVar;

  // Package modules.
  use CopyAggregation;
  use Search;

  // Arachne modules.
  use Graph;
  use EdgeCentricGraph;
  use VertexCentricGraph;
  use Aggregators;

  param profile:bool = true;

  /****************************************************************************/
  /****************************************************************************/
  /***************************VERTEX-CENTRIC BFS METHODS***********************/
  /****************************************************************************/
  /****************************************************************************/
  /*
    Edge counts are needed to calculate traversed-edges-per-second (TEPs). The
    validation step will check of this count is correct. The `array` can be 
    either a parent array or a level/depth array.
  */
  proc getEdgeCountForTeps(const ref array, graph) {
    var edgeCount = 0;
    forall (i,p) in zip(array.domain, array) with (+ reduce edgeCount){
      if p != -1 {
        for j in graph.neighborsInternal(i) do edgeCount += 1;
      }
    }
    return edgeCount;
  }

  /*
    Helper method to convert parent array to level array. Assumes passed source
    is the internal representation of the vertex.
  */
  proc parentToLevel(in parent, source) {
    var level = blockDist.createArray(parent.domain, int);
    var visited = blockDist.createArray(parent.domain, bool);
    var currLevel = 0;
    level = -1;
    visited = false;

    var frontiers: [{0..1}] list(int, parSafe=true);
    frontiers[0] = new list(int, parSafe=true);
    frontiers[1] = new list(int, parSafe=true);
    frontiersIdx = 0;
    frontiers[frontiersIdx].pushBack(source);

    forall (p,v) in zip(parent, visited) do if p != -1 then v = true;
    var visitedReduced = || reduce visited;
    while visitedReduced {
      coforall loc in Locales 
      with (ref level, ref parent, ref frontiers) 
      do on loc {
        forall u in frontiers[frontiersIdx] {
          level[u] = currLevel;
          visited[u] = false;
          parent[u] = -1;
        }
        var lo = parent.localSubdomain(loc).low;
        var hi = parent.localSubdomain(loc).high;
        const ref lSlice = parent.localSlice(lo..hi);
        forall (v,d) in zip(lSlice,lo..hi) {
          var isFound = frontiers[frontiersIdx].find(v);
          if isFound != -1 then frontiers[(frontiersIdx + 1) % 2].pushBack(d);
        }
      }
      currLevel += 1;
      frontiers[frontiersIdx].clear();
      frontiersIdx = (frontiersIdx + 1) % 2;
      visitedReduced = || reduce visited;
    }
    return level;
  }

  proc bfsParentVertexAgg(inGraph: shared Graph, internalSource:int) {
    var graph = toVertexCentricGraph(inGraph);
    var lo = graph.vertexMapper.domain.low;
    var hi = graph.vertexMapper.domain.high;
    var timer:stopwatch;
    
    if profile then timer.start();
    coforall loc in Locales with (ref frontiers) do on loc {
      frontiers[0] = new list(int, parSafe=true);
      frontiers[1] = new list(int, parSafe=true);
    }
    if profile {
      writef("Time for frontier initilization is %dr\n", timer.elapsed());
      timer.restart();
    }

    // Change parentsMA and visitedMA to match current graph dimensions.
    SpecialtyVertexDist.redistribute({lo..hi});
    SpecialtyVertexDom = {lo..hi};
    forall a in visitedMA do a.write(false);
    parentsMA = -1;
    frontiersIdx = 0;
    if profile {
      writef("Time for visited and parent initilization is %dr\n", timer.elapsed());
      timer.restart();
    }
    
    var frontierSize:int = 0;
    on graph.findLoc(internalSource) {
      frontiers[frontiersIdx].pushBack(internalSource);
      visitedMA[internalSource].write(true);
      parentsMA[internalSource] = internalSource;
    }
    frontierSize = 1;
    if profile {
      writef("Time for visiting source is %dr\n", timer.elapsed());
      timer.restart();
    }

    while frontierSize > 0 {
      frontierSize = 0;
      var innerTimer:stopwatch;
      if profile then innerTimer.start();
      coforall loc in Locales with (+ reduce frontierSize) 
      do on loc {
        frontierSize += frontiers[frontiersIdx].size;
        var localeTimer:stopwatch;
        if profile then localeTimer.start();
        forall u in frontiers[frontiersIdx] 
        with (var frontierAgg = new SpecialtyVertexDstAggregator((int,int))) {
          for v in graph.neighborsInternal(u) do
            frontierAgg.copy(graph.findLocNewer(v), (v,u));
        }
        frontiers[frontiersIdx].clear();
        if profile {
          writef("Time on locale %i expanding frontier of size %i is %dr\n", 
                  here.id, frontierSize, localeTimer.elapsed());
          localeTimer.reset();
        }
      }
      frontiersIdx = (frontiersIdx + 1) % 2;
      if profile && frontierSize != 0 {
        writef("Time for BFS iteration is %dr\n", innerTimer.elapsed());
        innerTimer.reset();
      }
    }
    return parentsMA;
  }

  proc bfsLevelVertexAgg(inGraph: shared Graph, internalSource:int) {
    var graph = toVertexCentricGraph(inGraph);

    coforall loc in Locales with(ref frontiers) do on loc {
      frontiers[0] = new list(int, parSafe=true);
      frontiers[1] = new list(int, parSafe=true);
    }
    frontiersIdx = 0;

    on graph.findLoc(internalSource) {
      frontiers[frontiersIdx].pushBack(internalSource);
    }
    var currLevel = 0; 
    var level = blockDist.createArray(graph.vertexMapper.domain, int);
    level = -1;
    var frontierSize = 1;

    // Declare global visited bitmap to track if a vertex has been visited or not.
    var visitedD = blockDist.createDomain(graph.vertexMapper.domain);
    var visited: [visitedD] chpl__processorAtomicType(bool);

    while frontierSize > 0 {
      frontierSize = 0;
      coforall loc in Locales with (+ reduce frontierSize) do on loc {
        frontierSize += frontiers[frontiersIdx].size;
        forall u in frontiers[frontiersIdx] 
        with (var frontierAgg = new listDstAggregator(int)) 
        {
          if !visited[u].testAndSet() {
            level[u] = currLevel;
            for v in graph.neighborsInternal(u) do 
              frontierAgg.copy(graph.findLocNewer(v), v);
          }
        }
        frontiers[frontiersIdx].clear();
      }
      currLevel += 1;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return level;
  }

  proc bfsParentVertexJenkins(inGraph: shared Graph, internalSource:int) {
    use DynamicArray;
    use RangeChunk;
    var graph = toVertexCentricGraph(inGraph);
    var lo = graph.vertexMapper.domain.low;
    var hi = graph.vertexMapper.domain.high;

    var globalWorkDom = {0..1} dmapped new replicatedDist();
    var globalWork: [globalWorkDom] Array(2*int);
    var globalWorkIdx: int;

    // Create  parents and visited to match current graph dimensions.
    var arrDist = new blockDist({lo..hi});
    var arrDom = {lo..hi} dmapped arrDist;
    var parents: [arrDom] int;
    var visited: [arrDom] bool;
    visited = false;
    parents = -1;
    globalWorkIdx = 0;
    
    var frontierSize:int = 0;
    on graph.findLoc(internalSource) {
      globalWork[globalWorkIdx].append((internalSource,internalSource));
    }
    frontierSize = 1;

    while frontierSize > 0 {
      var pendingWork = false;
      coforall loc in Locales with (|| reduce pendingWork) do on loc {
        var localeWork: [LocaleSpace] Array(2*int);
        ref workQueue = globalWork[globalWorkIdx]; 
        coforall chunk in chunks(0..#workQueue.size, numChunks=here.maxTaskPar)
        with (||reduce pendingWork) {
          var localWork: [LocaleSpace] Array(2*int);
          for u in workQueue[chunk] {
            if visited[u[0]] == false {
              visited[u[0]] = true;
              parents[u[0]] = u[1];
              pendingWork = true;
            }
            for v in graph.neighborsInternal(u[0]) do
              localWork[graph.findLocNewer(v)].append((v,u[0]));
          }
          for (_localeWork, _localWork) in zip(localeWork, localWork) {
            _localeWork.lock.acquire();
            _localeWork.append(_localWork);
            _localeWork.lock.release();
          }
        }
        coforall loc in Locales do on loc {
          globalWork[(globalWorkIdx+1)%2].lock.acquire();
          globalWork[(globalWorkIdx+1)%2].append(localeWork[here.id]);
          globalWork[(globalWorkIdx+1)%2].lock.release();
        }
      }
      globalWork[globalWorkIdx].clear();
      globalWorkIdx = (globalWorkIdx + 1) % 2;
      if !pendingWork then break;
    }
    return parents;
  }

  proc bfsParentVertexGraph500(inGraph: shared Graph, internalSource:int) {
    use DynamicArray;
    var graph = toVertexCentricGraph(inGraph);
    var lo = graph.vertexMapper.domain.low;
    var hi = graph.vertexMapper.domain.high;

    coforall loc in Locales with (ref queues) do on loc {
      queues[0] = new Array(int);
      queues[1] = new Array(int);
    }

    // Change parents and visited to match current graph dimensions.
    arrDist.redistribute({lo..hi});
    arrDom = {lo..hi};
    visited = false;
    parents = -1;
    queueIdx = 0;
    
    var frontierSize:int = 0;
    on graph.findLoc(internalSource) {
      queues[queueIdx].append(internalSource);
      visited[internalSource] = true;
      parents[internalSource] = internalSource;
    }
    frontierSize = 1;

    while frontierSize > 0 {
      frontierSize = 0;
      coforall loc in Locales with (+ reduce frontierSize) do on loc {
        frontierSize += queues[queueIdx].size;
        forall u in queues[queueIdx] with (var agg = new DynamicArrayDstAggregator((int,int))) {
          for v in graph.neighborsInternal(u) do
            agg.copy(graph.findLocNewer(v), (v,u));
        }
        queues[queueIdx].clear();
      }
      queueIdx = (queueIdx + 1) % 2;
    }
    return parents;
  }

  proc bfsLevelVertex(inGraph: shared Graph, internalSource:int) {
    var graph = toVertexCentricGraph(inGraph);

    var frontiers: [{0..1}] list(int, parSafe=true);
    frontiers[0] = new list(int, parSafe=true);
    frontiers[1] = new list(int, parSafe=true);

    frontiersIdx = 0;
    var currLevel = 0;
    frontiers[frontiersIdx].pushBack(internalSource);
    
    var level = blockDist.createArray(graph.vertexMapper.domain, int);
    level = -1;

    while true {
      var pendingWork:bool;
      forall u in frontiers[frontiersIdx] with (|| reduce pendingWork) {
        if level[u] == -1 {
          level[u] = currLevel;
          for v in graph.neighborsInternal(u) do 
            frontiers[(frontiersIdx + 1) % 2].pushBack(v);
          pendingWork = true;
        }
      }
      frontiers[frontiersIdx].clear();
      if !pendingWork then break;
      currLevel += 1;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return level;
  }

  proc bfsAggregationVertex(inGraph: shared Graph, internalSource:int) {
    var graph = toVertexCentricGraph(inGraph);

    coforall loc in Locales with(ref frontiers) do on loc {
      frontiers[0] = new list(int, parSafe=true);
      frontiers[1] = new list(int, parSafe=true);
    }
    frontiersIdx = 0;

    on graph.findLoc(internalSource) {
      frontiers[frontiersIdx].pushBack(internalSource);
    }
    var currLevel = 0; 
    var depth = blockDist.createArray(graph.vertexMapper.domain, int);
    depth = -1;
    depth[internalSource] = currLevel;

    while true {
      var pendingWork:bool;
      coforall loc in Locales 
      with (|| reduce pendingWork, ref depth, ref frontiers) 
      do on loc {
        forall u in frontiers[frontiersIdx] 
        with (|| reduce pendingWork, var frontierAgg=new listDstAggregator(int)) 
        {
          for v in graph.neighborsInternal(u) {
            if depth[v] == -1 {
              pendingWork = true;
              depth[v] = currLevel + 1; 
              frontierAgg.copy(graph.findLoc(v).id, v);
            }
          }
        }
        frontiers[frontiersIdx].clear();
      }
      if !pendingWork then break;
      currLevel += 1;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return depth;
  }

  proc topDownFine(ref currentFrontier, ref nextFrontier, ref depth, 
                   inGraph: shared Graph, currLevel:int): bool {
    var graph = toVertexCentricGraph(inGraph);
    var pendingWork:bool;
    forall u in currentFrontier with (|| reduce pendingWork, ref nextFrontier) {
      for v in graph.neighborsInternal(u) {
        if depth[v] == -1 {
          pendingWork = true;
          depth[v] = currLevel + 1;
          nextFrontier.add(v);
        }
      }
    }
    return pendingWork;
  }

  proc bottomUpFine(ref currentFrontier, ref nextFrontier, ref depth, 
                   inGraph: shared Graph, currLevel:int): bool {
    var graph = toVertexCentricGraph(inGraph);
    var pendingWork:bool;
    forall v in graph.vertexMapper.domain with (|| reduce pendingWork, ref nextFrontier) {
      if depth[v] == -1 {
        for u in graph.neighborsInternal(v) {
          if currentFrontier[u] {
            pendingWork = true;
            depth[v] = currLevel + 1;
            nextFrontier[v] = true;
          }
        }
      }
    }
    return pendingWork;
  }

  proc testAndSwitch(inGraph: shared Graph, ref currentFrontier, param ftype,
                     alpha:real, beta:real) {
    var graph = toVertexCentricGraph(inGraph);
    var mf, mu, nf, n: int;

    nf = currentFrontier.size;
    n = graph.vertexMapper.size;

    if ftype == "top" {
      forall v in currentFrontier with (+ reduce mf) do
        mf += graph.adjacencies[v].neighbors.size;
    }
    else {
      forall (i,v) in zip(currentFrontier.domain, currentFrontier) with (+ reduce mf) do
        if v then mf += graph.adjacencies[i].neighbors.size;
    }

    mu = graph.numEdges - mf;

    if mf > (mu / alpha) then return 0;
    else if nf < (n / beta) then return 1;
    else return -1;
  }

  /*
    Experimental breadth-first search version that requires the frontiers to be
    sets to enable a hybrid neighborhood expansion mechanism swapping between
    top-down and bottom-up neighborhood expansion as needed.

    **!!THERE ARE CORRECTNESS ERRORS FOR RMAT GRAPHS!!**
  */
  proc bfsNoAggregationVertexHybrid(inGraph: shared Graph, source:int, 
                                    alpha, beta) {
    var graph = toVertexCentricGraph(inGraph);
    var placeholder = blockDist.createArray(graph.vertexMapper.domain, bool);

    var frontiers: [{0..1}] set(int, parSafe=true);
    frontiers[0] = new set(int, parSafe=true);
    frontiers[1] = new set(int, parSafe=true);

    var frontiersB: [{0..1}] placeholder.type;
    frontiersB[0] = blockDist.createArray(graph.vertexMapper.domain, bool);
    frontiersB[1] = blockDist.createArray(graph.vertexMapper.domain, bool);

    var frontiersIdx = 0; 
    var currLevel = 0;
    var internalSource = binarySearch(graph.vertexMapper, source)[1];
    frontiers[frontiersIdx].add(internalSource);

    var depth = blockDist.createArray(graph.vertexMapper.domain, int);
    depth = -1;
    depth[internalSource] = currLevel;
    var neighborhoodExpansionType = 1;

    while true {
      var pendingWork:bool;
      if neighborhoodExpansionType == 0 {
        // change to bit map
        forall v in frontiers[frontiersIdx] do 
          frontiersB[frontiersIdx][v] = true;

        pendingWork = bottomUpFine(frontiersB[frontiersIdx], 
                                   frontiersB[(frontiersIdx + 1) % 2],
                                   depth, inGraph, currLevel
                                  );

        frontiersB[frontiersIdx] = false;
        neighborhoodExpansionType = testAndSwitch(inGraph, 
                                                  frontiersB[(frontiersIdx + 1) % 2], "bottom",
                                                  alpha, beta);
      }
      else if neighborhoodExpansionType == 1 {
        // change to set
        forall v in frontiersB[frontiersIdx] do 
          frontiers[frontiersIdx].add(v);

        pendingWork = topDownFine(frontiers[frontiersIdx], 
                                  frontiers[(frontiersIdx + 1) % 2],
                                  depth, inGraph, currLevel
                                );
        
        frontiers[frontiersIdx].clear();
        neighborhoodExpansionType = testAndSwitch(inGraph, 
                                                  frontiers[(frontiersIdx + 1) % 2], "top",
                                                  alpha, beta);
      }
      else {
        break;
      }      
      if !pendingWork then break;
      currLevel += 1;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return depth;
  }

  proc bfsNoAggregationVertex(inGraph: shared Graph, internalSource:int) {
    var graph = toVertexCentricGraph(inGraph);

    var frontiers: [{0..1}] list(int, parSafe=true);
    frontiers[0] = new list(int, parSafe=true);
    frontiers[1] = new list(int, parSafe=true);

    var frontiersIdx = 0; 
    var currLevel = 0;
    frontiers[frontiersIdx].pushBack(internalSource);

    var depth = blockDist.createArray(graph.vertexMapper.domain, int);
    depth = -1;
    depth[internalSource] = currLevel;

    while true {
      var pendingWork:bool;
      forall u in frontiers[frontiersIdx] with (|| reduce pendingWork) {
        for v in graph.neighborsInternal(u) {
          if depth[v] == -1 {
            pendingWork = true;
            depth[v] = currLevel + 1;
            frontiers[(frontiersIdx + 1) % 2].pushBack(v);
          }
        }
      }
      frontiers[frontiersIdx].clear();
      if !pendingWork then break;
      currLevel += 1;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return depth;
  }

  /****************************************************************************/
  /****************************************************************************/
  /***************************EDGE-CENTRIC BFS METHODS*************************/
  /****************************************************************************/
  /****************************************************************************/
  proc bfsParentEdgeAgg(inGraph: shared Graph, internalSource:int) {
    var graph = toEdgeCentricGraph(inGraph);

    coforall loc in Locales do on loc {
      var lo = graph.edgeRangesPerLocale[loc.id][0];
      var hi = graph.edgeRangesPerLocale[loc.id][2];

      fDBA[0].D = {lo..hi};
      fDBA[1].D = {lo..hi};
      fDBA[0].A = false;
      fDBA[1].A = false;
      parents1(1).D = {lo..hi};
      parents1(1).A = -1;
    }
    frontiersIdx = 0;

    for lc in graph.findLocs(internalSource) {
      on lc {
        fDBA[frontiersIdx].A[internalSource] = true;
        parents1(1).A[internalSource] = internalSource;
      }
    }
    
    while true {
      var pendingWork:bool;
      coforall loc in Locales 
      with (|| reduce pendingWork, ref fDBA) 
      do on loc {
        forall (u,d) in zip(fDBA[frontiersIdx].A,fDBA[frontiersIdx].D) 
        with (|| reduce pendingWork, 
              var frontierAgg = new SpecialtyEdgeDstAggregator((int,int))) {
          if u {
            for v in graph.neighborsInternal(d) {
              var locs = graph.findLocs(v);
              for lc in locs do frontierAgg.copy(lc.id, (v,d));
            }
            pendingWork = true;
          }
        }
        fDBA[frontiersIdx].A = false;
      }
      if !pendingWork then break;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return parentsToBlockDistParents(graph.vertexMapper.size);
  }

  proc bfsLevelEdgeAgg(inGraph: shared Graph, internalSource:int) {
    var graph = toEdgeCentricGraph(inGraph);

    coforall loc in Locales with(ref frontiers) do on loc {
      frontiers[0] = new list(int, parSafe=true);
      frontiers[1] = new list(int, parSafe=true);
    }
    frontiersIdx = 0;

    for lc in graph.findLocs(internalSource) {
      on lc do frontiers[frontiersIdx].pushBack(internalSource);
    }
    var currLevel = 0; 
    var level = blockDist.createArray(graph.vertexMapper.domain, int);
    level = -1;

    while true {
      var pendingWork:bool;
      coforall loc in Locales 
      with (|| reduce pendingWork, ref level, ref frontiers) 
      do on loc {
        forall u in frontiers[frontiersIdx] 
        with (|| reduce pendingWork, var frontierAgg=new listDstAggregator(int)) 
        {
          if level[u] == -1 {
            level[u] = currLevel;
            for v in graph.neighborsInternal(u) {
              var locs = graph.findLocs(v);
              for lc in locs do frontierAgg.copy(lc.id, v);
            }
            pendingWork = true;
          }
        }
        frontiers[frontiersIdx].clear();
      }
      if !pendingWork then break;
      currLevel += 1;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return level;
  }

  proc bfsLevelEdge(inGraph: shared Graph, internalSource:int) {
    var graph = toEdgeCentricGraph(inGraph);

    var frontiers: [{0..1}] list(int, parSafe=true);
    frontiers[0] = new list(int, parSafe=true);
    frontiers[1] = new list(int, parSafe=true);

    frontiersIdx = 0;
    var currLevel = 0;
    frontiers[frontiersIdx].pushBack(internalSource);
    
    var level = blockDist.createArray(graph.vertexMapper.domain, int);
    level = -1;

    while true {
      var pendingWork:bool;
      forall u in frontiers[frontiersIdx] with (|| reduce pendingWork) {
        if level[u] == -1 {
          level[u] = currLevel;
          for v in graph.neighborsInternal(u) do 
            frontiers[(frontiersIdx + 1) % 2].pushBack(v);
          pendingWork = true;
        }
      }
      frontiers[frontiersIdx].clear();
      if !pendingWork then break;
      currLevel += 1;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return level;
  }

  proc bfsNoAggregationEdge(inGraph: shared Graph, internalSource:int) {
    var graph = toEdgeCentricGraph(inGraph);

    var frontiers: [{0..1}] list(int, parSafe=true);
    frontiers[0] = new list(int, parSafe=true);
    frontiers[1] = new list(int, parSafe=true);

    var frontiersIdx = 0; 
    var currLevel = 0;
    frontiers[frontiersIdx].pushBack(internalSource);

    var depth = blockDist.createArray(graph.vertexMapper.domain, int);
    depth = -1;
    depth[internalSource] = currLevel;

    while true {
      var pendingWork:bool;
      forall u in frontiers[frontiersIdx] with (|| reduce pendingWork) {
        for v in graph.neighborsInternal(u) {
          if depth[v] == -1 {
            pendingWork = true;
            depth[v] = currLevel + 1;
            frontiers[(frontiersIdx + 1) % 2].pushBack(v);
          }
        }
      }
      frontiers[frontiersIdx].clear();
      if !pendingWork then break;
      currLevel += 1;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return depth;
  }

  proc bfsAggregationEdge(inGraph: shared Graph, internalSource:int) {
    var graph = toEdgeCentricGraph(inGraph);

    coforall loc in Locales with(ref frontiers) do on loc {
      frontiers[0] = new list(int, parSafe=true);
      frontiers[1] = new list(int, parSafe=true);
    }
    frontiersIdx = 0;

    for lc in graph.findLocs(internalSource) {
      on lc do frontiers[frontiersIdx].pushBack(internalSource);
    }
    var currLevel = 0; 
    var depth = blockDist.createArray(graph.vertexMapper.domain, int);
    depth = -1;
    depth[internalSource] = currLevel;

    while true {
      var pendingWork:bool;
      coforall loc in Locales
      with (|| reduce pendingWork, ref depth, ref frontiers)
      do on loc {
        forall u in frontiers[frontiersIdx] 
        with (|| reduce pendingWork, var frontierAgg=new listDstAggregator(int))
        {
          for v in graph.neighborsInternal(u, ensureLocal=true) {
            if depth[v] == -1 {
              pendingWork = true;
              depth[v] = currLevel + 1; 
              var locs = graph.findLocs(v);
              for lc in locs do frontierAgg.copy(lc.id, v);
            }
          }
        }
        frontiers[frontiersIdx].clear();
      }
      if !pendingWork then break;
      currLevel += 1;
      frontiersIdx = (frontiersIdx + 1) % 2;
    }
    return depth;
  }
}
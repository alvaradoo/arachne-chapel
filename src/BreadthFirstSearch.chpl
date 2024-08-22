module BreadthFirstSearch {
  // Chapel standard modules.
  use List;
  use Set;
  use Time;
  use BlockDist;
  use ReplicatedDist;
  use ReplicatedVar;

  // Chapel package modules.
  use CopyAggregation;
  use AggregationPrimitives;
  use Search;

  // Arachne modules.
  use Graph;
  use EdgeList;
  use VertexCentricGraph;
  use BreadthFirstSearchAggregators;

  param profile:bool = false;

  proc bfsLevelVertexAgg(inGraph: shared Graph, internalSource:int) {
    var graph = toVertexCentricGraph(inGraph);

    coforall loc in Locales with(ref levelFrontiers) do on loc {
      levelFrontiers[0] = new list(int, parSafe=true);
      levelFrontiers[1] = new list(int, parSafe=true);
    }
    levelFrontiersIdx = 0;

    on graph.findLoc(internalSource) {
      levelFrontiers[levelFrontiersIdx].pushBack(internalSource);
    }
    var currLevel = 0; 
    var level = blockDist.createArray(graph.vertexMapper.domain, int);
    level = -1;
    var levelFrontierSize = 1;

    // Declare global visited bitmap to track if a vertex has been visited or not.
    var visitedD = blockDist.createDomain(graph.vertexMapper.domain);
    var visited: [visitedD] chpl__processorAtomicType(bool);

    while levelFrontierSize > 0 {
      levelFrontierSize = 0;
      coforall loc in Locales with (+ reduce levelFrontierSize) do on loc {
        levelFrontierSize += levelFrontiers[levelFrontiersIdx].size;
        forall u in levelFrontiers[levelFrontiersIdx] 
        with (var frontierAgg = new LevelDstAggregator(int)) 
        {
          if !visited[u].testAndSet() {
            level[u] = currLevel;
            for v in graph.neighborsInternal(u) do 
              frontierAgg.copy(graph.findLocNewer(v), v);
          }
        }
        levelFrontiers[levelFrontiersIdx].clear();
      }
      currLevel += 1;
      levelFrontiersIdx = (levelFrontiersIdx + 1) % 2;
    }
    return level;
  }

  proc bfsParentVertexAgg(inGraph: shared Graph, internalSource:int) {
    var graph = toVertexCentricGraph(inGraph);
    var lo = graph.vertexMapper.domain.low;
    var hi = graph.vertexMapper.domain.high;
    var timer:stopwatch;
    
    if profile then timer.start();
    coforall loc in Locales with (ref parentFrontiers) do on loc {
      parentFrontiers[0] = new list(int, parSafe=true);
      parentFrontiers[1] = new list(int, parSafe=true);
    }
    if profile {
      writef("Time for frontier initilization is %dr\n", timer.elapsed());
      timer.restart();
    }

    // Change parents and visited to match current graph dimensions.
    SpecialtyVertexDist.redistribute({lo..hi});
    SpecialtyVertexDom = {lo..hi};
    forall a in visited do a.write(false);
    parents = -1;
    parentFrontiersIdx = 0;
    if profile {
      writef("Time for visited and parent initilization is %dr\n", timer.elapsed());
      timer.restart();
    }
    
    var parentFrontierSize:int = 0;
    on graph.findLoc(internalSource) {
      parentFrontiers[parentFrontiersIdx].pushBack(internalSource);
      visited[internalSource].write(true);
      parents[internalSource] = internalSource;
    }
    parentFrontierSize = 1;
    if profile {
      writef("Time for visiting source is %dr\n", timer.elapsed());
      timer.restart();
    }

    while parentFrontierSize > 0 {
      parentFrontierSize = 0;
      var innerTimer:stopwatch;
      if profile then innerTimer.start();
      coforall loc in Locales with (+ reduce parentFrontierSize) 
      do on loc {
        parentFrontierSize += parentFrontiers[parentFrontiersIdx].size;
        var localeTimer:stopwatch;
        if profile then localeTimer.start();
        forall u in parentFrontiers[parentFrontiersIdx] 
        with (var frontierAgg = new ParentDstAggregator((int,int))) {
          for v in graph.neighborsInternal(u) do
            frontierAgg.copy(graph.findLocNewer(v), (v,u));
        }
        parentFrontiers[parentFrontiersIdx].clear();
        if profile {
          writef("Time on locale %i expanding frontier of size %i is %dr\n", 
                  here.id, parentFrontierSize, localeTimer.elapsed());
          localeTimer.reset();
        }
      }
      parentFrontiersIdx = (parentFrontiersIdx + 1) % 2;
      if profile && parentFrontierSize != 0 {
        writef("Time for BFS iteration is %dr\n", innerTimer.elapsed());
        innerTimer.reset();
      }
    }
    return parents;
  }

  proc bfsLevelVertex(inGraph: shared Graph, internalSource:int) {
    var graph = toVertexCentricGraph(inGraph);

    var frontiers: [{0..1}] list(int, parSafe=true);
    frontiers[0] = new list(int, parSafe=true);
    frontiers[1] = new list(int, parSafe=true);

    var frontiersIdx = 0;
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
}
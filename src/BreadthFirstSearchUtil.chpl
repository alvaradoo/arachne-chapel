module BreadthFirstSearchUtil {
  // Chapel standard modules.
  use List;
  use BlockDist;

  // Chapel package modules.

  // Arachne modules.

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
    var frontiersIdx = 0;
    frontiers[0] = new list(int, parSafe=true);
    frontiers[1] = new list(int, parSafe=true);
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
}
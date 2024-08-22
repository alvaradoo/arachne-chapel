use VertexCentricGraph;
use EdgeCentricGraph;
use BlockDist;
use List;
use Map;

var src = blockDist.createArray({0..17}, int);
var dst = blockDist.createArray({0..17}, int);

src = [0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 9, 9, 10, 10, 10, 9];
dst = [0, 2, 3, 4, 4, 5, 9, 6, 7, 7, 8, 9, 9, 9, 11, 12, 15, 10];

var eGraph = new shared EdgeCentricGraph(src, dst);
var vGraph = new shared VertexCentricGraph(eGraph);

writeln("Internal neighbors function test:");
for (uei, uvi) in zip(eGraph.vertexMapper.domain, vGraph.vertexMapper.domain) {
  var eGraphAdjList = new list(int);
  var vGraphAdjList = new list(int);

  for v in eGraph.neighborsInternal(uei) do eGraphAdjList.pushBack(v);
  for v in vGraph.neighborsInternal(uvi) do vGraphAdjList.pushBack(v);

  writeln(uei:string, " from edge graph matches ", uvi:string, 
          " from vertex graph: ", eGraphAdjList == vGraphAdjList);
}
writeln();

writeln("Neighbors function test:");
for (ue, uv) in zip(eGraph.vertexMapper, vGraph.vertexMapper) {
  var eGraphAdjList = new list(int);
  var vGraphAdjList = new list(int);

  for v in eGraph.neighbors(ue) do eGraphAdjList.pushBack(v);
  for v in vGraph.neighbors(uv) do vGraphAdjList.pushBack(v);

  writeln(ue:string, " from edge graph matches ", uv:string, 
          " from vertex graph: ", eGraphAdjList == vGraphAdjList);
}
writeln();

writeln("Number of edges and vertices test:");
writeln("Vertices: ", eGraph.numVertices == vGraph.numVertices);
writeln("Edges: ", eGraph.numEdges == vGraph.numEdges);
writeln();

writeln("ensureLocal flag for edge graph test:");
var vertexToNeighborsMap = new map(int, list(int));
for u in eGraph.vertexMapper do vertexToNeighborsMap.add(u, new list(int));
for loc in Locales do on loc {
  for u in eGraph.vertexMapper {
    vertexToNeighborsMap[u].pushBack(eGraph.neighbors(u, ensureLocal=true));
  }
}

for u in eGraph.vertexMapper {
  writeln(u:string, " from local edge graph neighbor slicing matches: ",
          vertexToNeighborsMap[u] == new list(eGraph.neighbors(u)));
}

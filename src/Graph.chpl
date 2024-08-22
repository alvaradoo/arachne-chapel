module Graph {
  use BlockDist;
  use ReplicatedDist;

  use EdgeList;
  use VertexCentricGraph;

  class Graph {
    var kind="unknown";
  }

  var placeholderReplicatedD = {0..1} dmapped new replicatedDist();
  var placeHolderReplicatedA: [placeholderReplicatedD] (int,locale,int);
  var placeHolderBlockA = blockDist.createArray({0..1}, int);
  var placeHolderBlockAVertex = blockDist.createArray({0..1}, vertex);

  proc toEdgeList(inGraph:Graph) {
    return try! inGraph:shared EdgeList(
      placeHolderBlockA.type,
      placeHolderBlockA.type,
      placeHolderBlockA.type,
      placeHolderBlockA.type,
      placeHolderReplicatedA.type,
      int,
      int
    );
  }

  proc toVertexCentricGraph(inGraph:Graph) {
    return try! inGraph:shared VertexCentricGraph(
      placeHolderBlockAVertex.type,
      placeHolderBlockA.type,
      int,
      int
    );
  }

  proc toGraph(inGraph) {
    return try! inGraph:shared Graph;
  }
}
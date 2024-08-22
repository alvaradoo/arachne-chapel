// Chapel standard modules.
use Random;

// Arachne modules.
use Graph;
use EdgeList;
use Generator;
use VertexCentricGraph;
use BreadthFirstSearch;
use BreadthFirstSearchUtil;

config const lowerScale = 4;
config const upperScale = 8;
config const eFactor = 16;
config const trials = 5;
config const debug = false;
var globalCheck:bool = true;

for scale in lowerScale..upperScale {
  var ns = 2**scale;
  var ms = 2**scale * eFactor;
  var edgeView = genRMATgraph(0.57,0.19,0.19,0.05,scale,ns,ms,2);
  var vertexView = new shared VertexCentricGraph(edgeView);

  var sourcesIdx:[1..trials] int;
  fillRandom(sourcesIdx, edgeView.vertexMapper.domain.first, 
                      edgeView.vertexMapper.domain.last);
  
  var final:bool;
  for s in sourcesIdx {
    var levelVertex = bfsLevelVertex(toGraph(vertexView), s); // ground truth
    var parentVertex = bfsParentVertex(toGraph(vertexView), s);
    var parentVertexAgg = bfsParentVertexAgg(toGraph(vertexView), s);
    var levelVertexAgg = bfsLevelVertexAgg(toGraph(vertexView), s);
    var p2LVertexAgg = parentToLevel(parentVertexAgg,s);
    var p2LVertex = parentToLevel(parentVertex,s);

    var parentVertexAggCheck = && reduce (p2LVertexAgg == levelVertex);
    var levelVertexAggCheck = && reduce (levelVertexAgg == levelVertex);
    var parentVertexCheck = && reduce (p2LVertex == levelVertex);

    final = globalCheck && parentVertexAggCheck && levelVertexAggCheck
                        && parentVertexCheck;

    if debug {
      writeln("Outputs for scale ", scale, " and source ", s, ": ");
      writeln("parentVertexAggCheck = ", parentVertexAggCheck);
      writeln("parentVertexCheck = ", parentVertexCheck);
      writeln("levelVertexAggCheck = ", levelVertexAggCheck);
      writeln();
    }
    
    if !final then halt("One of previous checks does not pass!");
  }
}
writeln("ALL TESTS PASSED.");

use Graph;
use Random;
use Search;
use Generator;
use Aggregators;
use EdgeCentricGraph;
use VertexCentricGraph;
use BreadthFirstSearch;

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
    var parentVertexGraph500 = bfsParentVertexGraph500(toGraph(vertexView), s);
    var parentVertexAgg = bfsParentVertexAgg(toGraph(vertexView), s);
    var levelVertexAgg = bfsLevelVertexAgg(toGraph(vertexView), s);
    var levelVertex = bfsLevelVertex(toGraph(vertexView), s); // ground truth
    var levelAggregationVertex = bfsAggregationVertex(toGraph(vertexView), s);
    var levelNoAggregationVertex = bfsNoAggregationVertex(toGraph(vertexView), s);

    var p2LGraph500 = parentToLevel(parentVertexGraph500,s);
    var p2LVertexAgg = parentToLevel(parentVertexAgg,s);

    var Graph500Check = && reduce (p2LGraph500 == levelVertex);
    var parentVertexAggCheck = && reduce (p2LVertexAgg == levelVertex);
    var levelVertexAggCheck = && reduce (levelVertexAgg == levelVertex);
    var levelAggregationVertexCheck = && reduce (levelAggregationVertex == levelVertex);
    var levelNoAggregationVertexCheck = && reduce (levelNoAggregationVertex == levelVertex);

    final = globalCheck && Graph500Check && parentVertexAggCheck 
                        && levelVertexAggCheck && levelAggregationVertexCheck
                        && levelNoAggregationVertexCheck;

    if debug {
      writeln("Outputs for scale ", scale, " and source ", s, ": ");
      writeln("Graph500Check = ", Graph500Check);
      writeln("parentVertexAggCheck = ", parentVertexAggCheck);
      writeln("levelVertexAggCheck = ", levelVertexAggCheck);
      writeln("levelAggregationVertexCheck = ", levelAggregationVertexCheck);
      writeln("levelNoAggregationVertexCheck = ", levelNoAggregationVertexCheck);
      writeln();
    }
    
    if !final then halt("One of previous checks does not pass!");
  }
}
writeln("ALL TESTS PASSED.");

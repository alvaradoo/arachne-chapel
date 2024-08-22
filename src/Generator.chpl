/*
  Contains functionality for generating RMAT graphs. These graphs follow a 
  power law distribution which means that a vertex or small subsets of vertices
  have a degree that is proportional to the power of degree of another vertex.
  This type of distribution is what is often found in real-world graphs.
*/
module Generator {
  use Random;
  use BlockDist;
  use EdgeCentricGraph;

  proc assignQuadrant(iiBit:bool, jjBit:bool, bit:int):(int,int) {
    var start, end:int = 0; 

    if !iiBit && !jjBit then; //do nothing;
    else if iiBit && !jjBit then start += 1; 
    else if !iiBit && jjBit then end += 1; 
    else { start = 1; end = 1; }

    return (bit*start, bit*end);
  }

  proc genRMATgraph(a:real, b:real, c:real, d:real, SCALE:int, nVERTICES:int,
                    nEDGES:int, maxEweight:int) {
    const vRange = 1..nVERTICES,
          eRange = 1..nEDGES;

    var randGen = new randomStream(real);

    var A = blockDist.createArray({eRange}, real),
        B = blockDist.createArray({eRange}, real),
        C = blockDist.createArray({eRange}, real),
        unifRandom = blockDist.createArray({eRange}, real),
        edges = blockDist.createArray({eRange}, (int,int));

    (A, B, C) = (a, b, c);
    edges = (1,1);
    var skip:real;
    for s in 1..SCALE {
      var cNorm = C / (1 - (A + B));
      var aNorm = A / (A + B);

      skip = randGen.next();
      randGen.fill(unifRandom);
      var iiBit = unifRandom > (A + B);

      skip = randGen.next();
      randGen.fill(unifRandom);
      var jjBit = unifRandom > (cNorm * iiBit:real + aNorm * (!iiBit):real);
      
      edges += assignQuadrant(iiBit, jjBit, 2**(s-1));
    }

    var permutation = blockDist.createArray({vRange}, atomic int);
    forall (p,i) in zip(permutation,permutation.domain) do p.write(i); 
    
    var eWeights = blockDist.createArray({eRange}, int);
    randGen.fill(unifRandom);
    eWeights = floor(1 + unifRandom * maxEweight):int;
    randGen.fill(unifRandom[vRange]);

    forall v in permutation.domain {
      var newID = floor(1 + unifRandom[v] * nVERTICES):int;
      var replacedVal = permutation[v].exchange(permutation[newID].read());
      permutation[newID].write(replacedVal);
    }

    forall e in edges.domain {
      edges[e][0] = permutation[edges[e][0]].read();
      edges[e][1] = permutation[edges[e][1]].read();
    }

    var src = blockDist.createArray({0..<nEDGES}, int);
    var dst = blockDist.createArray({0..<nEDGES}, int);

    forall (e,s,d) in zip(edges,src,dst) { (s,d) = e; }

    return new shared EdgeCentricGraph(src, dst);
  }
}
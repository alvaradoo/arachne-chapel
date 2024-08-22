/* Provides the aggregators used by the multilocale version of breadth-first
   search in the `BreadthFirstSearch` module.

   The two main records are `LevelDstAggregator` which is used by 
   `bfsLevelVertexAgg` and `ParentDstAggregator` which is used by
   `bfsParentvertexAgg`. These are separated because their `_flushBuffer` 
   methods are different. Currently, Chapel does not contain fleshed out
   support for user-defined aggregators. As time passes, if support for this
   grows, then we can refactor this code.
*/
module BreadthFirstSearchAggregators {
  // Chapel standard modules.
  use List;
  use Time;
  use BlockDist;
  use ReplicatedDist;
  use ReplicatedVar;
  
  // Chapel package modules.
  use CopyAggregation;
  use AggregationPrimitives;

  // Sizes of buffer and their yielding frequences
  private const dstBuffSize = getEnvInt("CHPL_AGGREGATION_DST_BUFF_SIZE", 
                                        4096);
  private const yieldFrequency = getEnvInt("CHPL_AGGREGATION_YIELD_FREQUENCY", 
                                           1024);
  
  /****************************************************************************/
  /*************************** BFS LEVEL AGGREGATOR ***************************/
  /****************************************************************************/
  // Declare our global frontier queues with parallel safety.
  var levelFrontierDom = {0..1} dmapped new replicatedDist();
  var levelFrontiers: [levelFrontierDom] list(int, parSafe=true);
  var levelFrontiersIdx:int;

  /*
    Record that aggregates writes to the frontiers that calculate the level
    of each vertex in relation to a source vertex.
  */
  record LevelDstAggregator {
    type eltType;
    type aggType = eltType;
    const bufferSize = dstBuffSize;
    const myLocaleSpace = LocaleSpace;
    var opsUntilYield = yieldFrequency;
    var lBuffers: [myLocaleSpace] [0..#bufferSize] aggType;
    var rBuffers: [myLocaleSpace] remoteBuffer(aggType);
    var bufferIdxs: [myLocaleSpace] int;

    proc ref postinit() {
      for loc in myLocaleSpace do 
        rBuffers[loc] = new remoteBuffer(aggType, bufferSize, loc);
    }

    proc ref deinit() { flush(); }

    proc ref flush() {
      for loc in myLocaleSpace do
        _flushBuffer(loc, bufferIdxs[loc], freeData=true);
    }

    inline proc ref copy(const loc, const in srcVal: eltType) {
      // Get identifier for buffer for specific locale.
      ref bufferIdx = bufferIdxs[loc];

      // Buffer the desired value. 
      lBuffers[loc][bufferIdx] = srcVal;
      bufferIdx += 1;

      // Flush buffer when it is full or yield if this task has exhausted its
      // yield count. In other words, if its yield count is low it might be 
      // block other remote tasks from flushing their buffers.
      if bufferIdx == bufferSize {
        _flushBuffer(loc, bufferIdx, freeData=false);
        opsUntilYield = yieldFrequency; 
      } else if opsUntilYield == 0 {
        currentTask.yieldExecution();
        opsUntilYield = yieldFrequency; 
      } else {
        opsUntilYield -= 1;
      }
    }

    proc ref _flushBuffer(loc: int, ref bufferIdx, freeData) {
      // Make bufferIdx constant and return if trying to buffer locally.
      const myBufferIdx = bufferIdx; 
      if myBufferIdx == 0 then return;

      // Get remote buffer and allocate that space, if it does not already
      // exist. The metho `cachedAlloc` is defined in the module 
      // `CopyAggregation`.
      ref rBuffer = rBuffers[loc];
      const remBufferPtr = rBuffer.cachedAlloc();

      // Put into rBuffer the contents of lBuffer.
      rBuffer.PUT(lBuffers[loc], myBufferIdx);
      
      // On the remote locale, populate frontier from rBuffer.
      on Locales[loc] {
        ref f = levelFrontiers[(levelFrontiersIdx + 1) % 2];
        for srcVal in rBuffer.localIter(remBufferPtr, myBufferIdx) do
          f.pushBack(srcVal);

        if freeData then rBuffer.localFree(remBufferPtr); // Free the memory.
      }
      if freeData then rBuffer.markFreed(); // Mark memory as freed.
      bufferIdx = 0;
    }
  }

  /****************************************************************************/
  /**************************** BFS PARENT AGGREGATOR *************************/
  /****************************************************************************/
  // Declare our global frontier queues with parallel safety.
  var parentFrontierDom = {0..1} dmapped new replicatedDist();
  var parentFrontiers: [parentFrontierDom] list(int, parSafe=true);
  var parentFrontiersIdx:int;

  // Declare default distribution.
  var SpecialtyVertexDist = new blockDist({0..<numLocales});
  var SpecialtyVertexDom  = {0..<numLocales} dmapped SpecialtyVertexDist;
  
  // Declare global visited bitmap to track if a vertex has been visited or not.
  var visited: [SpecialtyVertexDom] chpl__processorAtomicType(bool);

  // Declare global parents array to keep track of the parent of each vertex.
  var parents: [SpecialtyVertexDom] int;

  /*
    Record that aggregates writes to the frontiers that calculate the parent
    of each vertex in the breadth-first search tree in relation to a 
    source vertex.
  */
  record ParentDstAggregator {
    type eltType;
    type aggType = eltType;
    const bufferSize = dstBuffSize;
    const myLocaleSpace = LocaleSpace;
    var opsUntilYield = yieldFrequency;
    var lBuffers: [myLocaleSpace] [0..#bufferSize] aggType;
    var rBuffers: [myLocaleSpace] remoteBuffer(aggType);
    var bufferIdxs: [myLocaleSpace] int;
    
    proc ref postinit() {
      for loc in myLocaleSpace do 
        rBuffers[loc] = new remoteBuffer(aggType, bufferSize, loc);
    }

    proc ref deinit() { flush(); }

    proc ref flush() {
      for loc in myLocaleSpace do
        _flushBuffer(loc, bufferIdxs[loc], freeData=true);
    }

    inline proc ref copy(const loc, const in srcVal: eltType) {
      // Get identifier for buffer for specific locale.
      ref bufferIdx = bufferIdxs[loc];

      // Buffer the desired value. 
      lBuffers[loc][bufferIdx] = srcVal;
      bufferIdx += 1;

      // Flush buffer when it is full or yield if this task has exhausted its
      // yield count. In other words, if its yield count is low it might be 
      // block other remote tasks from flushing their buffers.
      if bufferIdx == bufferSize {
        _flushBuffer(loc, bufferIdx, freeData=false);
        opsUntilYield = yieldFrequency; 
      } else if opsUntilYield == 0 {
        currentTask.yieldExecution();
        opsUntilYield = yieldFrequency; 
      } else {
        opsUntilYield -= 1;
      }
    }

    proc ref _flushBuffer(loc: int, ref bufferIdx, freeData) {
      // Make bufferIdx constant and return if trying to buffer locally.
      const myBufferIdx = bufferIdx;
      if myBufferIdx == 0 then return;

      // Get remote buffer and allocate that space, if it does not already
      // exist. The method `cachedAlloc` is defined in the module 
      // `CopyAggregation`.
      ref rBuffer = rBuffers[loc];
      const remBufferPtr = rBuffer.cachedAlloc();

      // Put into rBuffer the contents of lBuffer.
      rBuffer.PUT(lBuffers[loc], myBufferIdx);

      // On the remote locale, populate frontier from rBuffer.
      on Locales[loc] {
        ref f = parentFrontiers[(parentFrontiersIdx + 1) % 2];
        for srcVal in rBuffer.localIter(remBufferPtr, myBufferIdx) {
          var (v,p) = srcVal;
          if !visited.localAccess[v].testAndSet() {
            parents.localAccess[v] = p;
            f.pushBack(v);
          }
        }
        if freeData then rBuffer.localFree(remBufferPtr); // Free the memory.
      }
      if freeData then rBuffer.markFreed(); // Mark memory as freed.
      bufferIdx = 0;
    }
  }
}
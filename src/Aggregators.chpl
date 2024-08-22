module Aggregators {
  use List;
  use ReplicatedDist;
  use ReplicatedVar;
  use BlockDist;
  use CopyAggregation;
  use AggregationPrimitives;

  // Sizes of buffer and their yielding frequences
  private const dstBuffSize = getEnvInt("CHPL_AGGREGATION_DST_BUFF_SIZE", 
                                        4096);
  private const yieldFrequency = getEnvInt("CHPL_AGGREGATION_YIELD_FREQUENCY", 
                                           1024);
  
  /****************************************************************************/
  /************************ LIST PUSHBACK AGGREGATOR **************************/
  /****************************************************************************/
  // Declare our global frontier queues with parallel safety.
  var Dfrontier = {0..1} dmapped new replicatedDist();
  var frontiers: [Dfrontier] list(int, parSafe=true);
  var frontiersIdx:int;

  // Declare our global frontier queues without parallel safety.
  var DfrontierSeq = {0..1} dmapped new replicatedDist();
  var frontiersSeq: [DfrontierSeq] list(int);
  
  /*
    Aggregator to be utilized with Chapel lists in breadth-first search. 
    Designed using works from:
    (1) the `DstAggregator` that is in Arkouda and 
    (2) https://chapel-lang.org/CHIUW/2021/Rolinger.pdf.
  */
  record listDstAggregator {
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
        ref f = frontiers[(frontiersIdx + 1) % 2];
        for srcVal in rBuffer.localIter(remBufferPtr, myBufferIdx) do
          f.pushBack(srcVal);

        if freeData then rBuffer.localFree(remBufferPtr); // Free the memory.
      }
      if freeData then rBuffer.markFreed(); // Mark memory as freed.
      bufferIdx = 0;
    }
  }

  /****************************************************************************/
  /*************************** MULTI-ARRAY AGGREGATOR *************************/
  /****************************************************************************/
  // Declare default distribution.
  var SpecialtyVertexDist = new blockDist({0..<numLocales});
  var SpecialtyVertexDom  = {0..<numLocales} dmapped SpecialtyVertexDist;
  
  // Declare global visited bitmap to track if a vertex has been visited or not.
  var visitedMA: [SpecialtyVertexDom] chpl__processorAtomicType(bool);

  // Declare global parents array to keep track of the parent of each vertex.
  var parentsMA: [SpecialtyVertexDom] int;

  record SpecialtyVertexDstAggregator {
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
        ref f = frontiers[(frontiersIdx + 1) % 2];
        for srcVal in rBuffer.localIter(remBufferPtr, myBufferIdx) {
          var (v,p) = srcVal;
          if !visitedMA.localAccess[v].testAndSet() {
            parentsMA.localAccess[v] = p;
            f.pushBack(v);
          }
        }
        if freeData then rBuffer.localFree(remBufferPtr); // Free the memory.
      }
      if freeData then rBuffer.markFreed(); // Mark memory as freed.
      bufferIdx = 0;
    }
  }

  /****************************************************************************/
  /*************************** STAGGERED AGGREGATOR ***************************/
  /****************************************************************************/
  record DynamicBoolArray {
    var D = {0..1};
    var A: [D] bool;
  }

  record Parents {
    var D = {0..1};
    var A: [D] int;
  }

  // Declare our global frontier queues.
  var fDBA: [Dfrontier] DynamicBoolArray;

  // Declare our per-locale parents1 array wrapper.
  var parents1: [rcDomain] Parents;

  proc parentsToBlockDistParents(n:int) {
    var blockParents = blockDist.createArray({0..<n}, int);

    coforall loc in Locales do on loc {
      forall (u,d) in zip(parents1(1).A, parents1(1).D) do blockParents[d] = u;
    }

    return blockParents;
  }

  record SpecialtyEdgeDstAggregator {
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
        ref f = fDBA[(frontiersIdx + 1) % 2];
        for srcVal in rBuffer.localIter(remBufferPtr, myBufferIdx) {
          if parents1(1).A[srcVal[0]] == -1 {
            parents1(1).A[srcVal[0]] = srcVal[1];
            f.A[srcVal[0]] = true;
          }
        }
        if freeData then rBuffer.localFree(remBufferPtr); // Free the memory.
      }
      if freeData then rBuffer.markFreed(); // Mark memory as freed.
      bufferIdx = 0;
    }
  }
}
module DynamicArray {
  use IO;
  use Sort;
  use BlockDist;
  use CyclicDist;
  use ReplicatedDist;
  use AggregationPrimitives;
  use CopyAggregation;
  use Time;

  config const arrayGrowthRate = 1.5;

  /*
    Simple lock used for mutual exclusion.
  */
  pragma "default intent is ref"
  record Lock {
    var _lock : chpl__processorAtomicType(bool);
    
    proc init() {
      this._lock = false;
    }
    
    inline proc ref acquire(){
      on this do local {
        if _lock.testAndSet() == true {
          while _lock.read() == true || _lock.testAndSet() == true {
            currentTask.yieldExecution();
          }
        }
      }
    }

    inline proc ref release() { on this do local do _lock.clear(); }
  } /* end Lock */

  pragma "default intent is ref"
  record Array {
    type eltType;
    var dom = {0..0};
    var arr : [dom] eltType;
    var sz : int;
    var cap : int = 1;
    var lock : Lock;

    proc ref preallocate(length : int) {
      if cap < length {
        this.cap = length;
        this.dom = {0..#this.cap};
      }
    }

    iter these() {
      if sz != 0 {
        if this.locale != here {
          var _dom = {0..#sz};
          var _arr : [_dom] eltType = arr;
          for a in arr[0..#sz] do yield a;
        } else {
          for a in arr[0..#sz] do yield a;
        }
      }
    }

    iter these(param tag : iterKind) where tag == iterKind.standalone {
      if sz != 0 then
      if this.locale != here {
        var _dom = {0..#sz};
        var _arr : [_dom] eltType = arr;
        forall a in arr[0..#sz] do yield a;
      } else {
        forall a in arr[0..#sz] do yield a;
      }
    }
      
    proc ref append(ref other : this.type) {
      const otherSz = other.sz;
      if otherSz == 0 then return;
      local { 
        if sz + otherSz > cap {
          this.cap = sz + otherSz;
          this.dom = {0..#cap};
        }
      }
      this.arr[sz..#otherSz] = other.arr[0..#otherSz];
      sz += otherSz;
    }

    proc ref append(elt : eltType) {
      if sz == cap {
        var oldCap = cap;
        cap = round(cap * arrayGrowthRate) : int;
        if oldCap == cap then cap += 1;
        this.dom = {0..#cap};
      }
      this.arr[sz] = elt;
      sz += 1;
    }

    inline proc this(idx) { return arr[idx]; }

    pragma "no copy return"
    proc getArray() { return arr[0..#sz]; }

    proc ref clear() { local do this.sz = 0; }

    proc ref size { return this.sz; }
  } // END Array


  // At the moment, to do the aggregation we need for BFS, we need to
  // have access to the queues in this module.
  var D_util = {0..1} dmapped new replicatedDist();
  var queues : [D_util] Array(int);
  var queueIdx : int;

  // Declare default distribution.
  var arrDist = new blockDist({0..<numLocales});
  var arrDom = {0..<numLocales} dmapped arrDist;
  
  // Declare global visited bytemap to track if a vertex has been visited.
  var visited: [arrDom] bool;

  // Declare global parents array to keep track of the parent of each vertex.
  var parents: [arrDom] int;

  //###############################################################################
  //###############################################################################
  //###############################################################################
  /*
      Custom Destination Aggregator for the dynamic arrays defined above
      (called Array).

      The operations we want to aggregate are the appends, where an array we
      are appending to corresponds to some locale's next-frontier and the
      elements we are appending are vertex IDs.
  
      The tricky part is that the copy-aggregation module has been designed
      for things like dst[idx] = val, where a tuple is buffered to represent
      the destination address and the value to write to. When the buffer is
      full for a given locale, those tuples are iterated over and the writes
      take place. But that isn't what we want to do for our case.

      We don't have a specific destination address to buffer, since we are just
      appending elements to the end of the array. But when we flush, we need to
      have access to the Array we are flushing to so we can get a lock and adjust
      its size. But we can't seem to store refs as fields in a record, so I am
      not sure how to do this.

      For now, we do a hack and make our BFS queues globally accessible here.
  */
  private const dstBuffSize = getEnvInt("CHPL_AGGREGATION_DST_BUFF_SIZE", 4096);
  private const yieldFrequency = getEnvInt("CHPL_AGGREGATION_YIELD_FREQUENCY", 1024);
  
  record DynamicArrayDstAggregator {
      type elemType;
      type aggType = elemType;
      const bufferSize = dstBuffSize;
      const myLocaleSpace = LocaleSpace;
      var opsUntilYield = yieldFrequency;
      var lBuffers: [myLocaleSpace] [0..#bufferSize] aggType;
      var rBuffers: [myLocaleSpace] remoteBuffer(aggType);
      var bufferIdxs: [myLocaleSpace] int;

      proc ref postinit() {
        for loc in myLocaleSpace {
          rBuffers[loc] = new remoteBuffer(aggType, bufferSize, loc);
        }
      }

      proc ref deinit() { flush(); }

      proc ref flush() {
        for loc in myLocaleSpace {
          _flushBuffer(loc, bufferIdxs[loc], freeData=true);
        }
      }

      inline proc ref copy(const loc, const in srcVal: elemType) {
        // Get our current index into the buffer for dst's locale
        ref bufferIdx = bufferIdxs[loc];

        // Buffer the desired value
        lBuffers[loc][bufferIdx] = srcVal;
        bufferIdx += 1;

        // Flush our buffer if it's full. If it's been a while since we've let
        // other tasks run, yield so that we're not blocking remote tasks from
        // flushing their buffers.
        if bufferIdx == bufferSize {
          _flushBuffer(loc, bufferIdx, freeData=false);
          opsUntilYield = yieldFrequency;
        } 
        else if opsUntilYield == 0 {
          currentTask.yieldExecution();
          opsUntilYield = yieldFrequency;
        } 
        else {
          opsUntilYield -= 1;
        }
      }

      // Flushes the buffer. This means doing a big append to the
      // Array instance. Since other tasks may be flushing to the same
      // Array, we need to atomically adjust the size of the Array before
      // the append. That way, this task can adjust the size of the array
      // to accomadate all of the elements it's going to append. 
      proc ref _flushBuffer(loc: int, ref bufferIdx, freeData) {
        const myBufferIdx = bufferIdx;
        if myBufferIdx == 0 then return;

        // Allocate a remote buffer
        ref rBuffer = rBuffers[loc];
        const remBufferPtr = rBuffer.cachedAlloc();

        // Copy local buffer to remote buffer
        rBuffer.PUT(lBuffers[loc], myBufferIdx);

        // Process remote buffer
        on Locales[loc] {
          queues[(queueIdx+1)%2].lock.acquire();
          ref q = queues[(queueIdx+1)%2];
          var curr_idx:int;
          var resized:bool = false;
          for srcVal in rBuffer.localIter(remBufferPtr, myBufferIdx) {
            var (v,p) = srcVal;
            if visited[v] == false {
              if !resized {
                const orig_size = q.sz;
                q.preallocate(orig_size + myBufferIdx);
                q.sz = orig_size + myBufferIdx;
                curr_idx = orig_size;
                resized = true;
              }
              visited[v] = true;
              parents[v] = p;
              q.arr[curr_idx] = v;
              curr_idx += 1;
            }
          }
          q.lock.release();
          if freeData then rBuffer.localFree(remBufferPtr);
        }
        if freeData then rBuffer.markFreed();
        bufferIdx = 0;
      }

  } /* end DynamicArrayDstAggregator */
}
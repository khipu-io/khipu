package khipu.util

object Sizeof {
  private val s_runtime = Runtime.getRuntime();

  def main(args: Array[String]) {
    // Warm up all classes/methods we will use
    runGC()
    usedMemory()
    // Array to keep strong references to allocated objects
    val count = 100000
    var objs = Array.ofDim[AnyRef](count)

    var heap1 = 0L
    // Allocate count+1 objects, discard the first one
    var i = -1
    while (i < count) {
      var obj = null //new Object()
      //var obj = new Object()
      //obj = new Integer (i)
      //obj = new Long (i)
      //obj = new String ()
      //obj = new byte [128][1]

      if (i >= 0) {
        objs(i) = obj
      } else {
        obj = null // Discard the warm up object
        runGC()
        heap1 = usedMemory() // Take a before heap snapshot
      }
      i += 1
    }

    runGC()
    val heap2 = usedMemory() // Take an after heap snapshot:

    val size = math.round((heap2 - heap1).toDouble / count);
    println("'before' heap: " + heap1 + ", 'after' heap: " + heap2);
    val clz = if (objs(0) == null) "null" else objs(0).getClass.toString
    println("heap delta: " + (heap2 - heap1) + ", {" + clz + "} size = " + size + " bytes");
    i = 0
    while (i < count) {
      objs(i) = null
      i += 1
    }
    objs = null
  }

  private def runGC() {
    (0 until 4) foreach (i => _runGC())
  }

  private def _runGC() {
    var usedMem1 = usedMemory()
    var usedMem2 = Long.MaxValue
    var i = 0
    while ((usedMem1 < usedMem2) && (i < 500)) {
      s_runtime.runFinalization()
      s_runtime.gc()
      Thread.`yield`()

      usedMem2 = usedMem1
      usedMem1 = usedMemory()
      i += 1
    }
  }

  private def usedMemory(): Long = {
    s_runtime.totalMemory() - s_runtime.freeMemory()
  }

}

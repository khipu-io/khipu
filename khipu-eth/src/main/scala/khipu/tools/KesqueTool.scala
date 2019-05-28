package khipu.tools

import java.io.File
import java.nio.ByteBuffer
import kesque.HashKeyValueTable
import kesque.Kesque
import kesque.TKeyVal
import khipu.Hash
import khipu.crypto
import scala.util.Random
import scala.collection.mutable

/**
 * Fill memory:
 * # stress -m 1 --vm-bytes 25G --vm-keep
 */
object KesqueTool {
  def main(args: Array[String]) {
    val dbtool = new KesqueTool()

    dbtool.test(1000000)
  }
}
class KesqueTool() {
  private def xf(n: Double) = "%1$10.1f".format(n)

  val khipuPath = new File(classOf[KesqueTool].getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile.getParentFile
  val configDir = new File(khipuPath, "../src/main/resources")

  val configFile = new File(configDir, "kafka.server.properties")
  val props = org.apache.kafka.common.utils.Utils.loadProps(configFile.getAbsolutePath)
  val kesque = new Kesque(props)

  val home = new File("/home/dcaoyuan/tmp")

  val tableName = "storage"
  if (!home.exists) {
    home.mkdirs()
  }

  def test(num: Int) = {
    val table = kesque.getTable(Array(tableName), fetchMaxBytes = 4096)

    val keys = write(table, num)
    read(table, keys)

    System.exit(0)
  }

  def write(table: HashKeyValueTable, num: Int) = {
    val keys = new java.util.ArrayList[Array[Byte]]()
    val start0 = System.nanoTime
    var start = System.nanoTime
    var elapsed = 0L
    var totalElapsed = 0L
    var i = 0
    val nKeysToRead = 1000000
    val keyInterval = math.max(num / nKeysToRead, 1)

    val kvs = new mutable.ArrayBuffer[TKeyVal]()
    while (i < num) {

      var j = 0
      while (j < 4000 && i < num) {
        val v = Array.ofDim[Byte](64)
        Random.nextBytes(v)
        val k = crypto.kec256(v)

        start = System.nanoTime

        val sKey = sliceBytes(k)
        kvs += TKeyVal(sKey, v, -1, -1)

        val duration = System.nanoTime - start
        elapsed += duration
        totalElapsed += duration

        if (i % keyInterval == 0) {
          keys.add(k)
        }

        j += 1
        i += 1
      }

      start = System.nanoTime

      table.write(kvs, tableName)

      val duration = System.nanoTime - start
      elapsed += duration
      totalElapsed += duration

      if (i > 0 && i % 100000 == 0) {
        val speed = 100000 / (elapsed / 1000000000.0)
        println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - write")
        start = System.nanoTime
        elapsed = 0L
      }
    }

    //val stats = table.getStats(null, null).asInstanceOf[HashStats]
    //println(s"stats: $stats")
    val speed = i / (totalElapsed / 1000000000.0)
    println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - write all in ${xf((totalElapsed / 1000000000.0))}s")

    keys
  }

  def read(table: HashKeyValueTable, keys: java.util.ArrayList[Array[Byte]]) {
    java.util.Collections.shuffle(keys)

    val start0 = System.nanoTime
    var start = System.nanoTime
    val itr = keys.iterator
    var i = 0
    while (itr.hasNext) {
      val k = itr.next
      val sKey = sliceBytes(k)

      // pseudo read only
      val value = table.read(sKey, tableName)

      if (i > 0 && i % 10000 == 0) {
        val elapsed = (System.nanoTime - start) / 1000000000.0 // sec
        val speed = 10000 / elapsed
        val hashKey = Hash(k)
        println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - 0x$hashKey")
        start = System.nanoTime
      }

      i += 1
    }

    val totalElapsed = (System.nanoTime - start0) / 1000000000.0 // sec
    val speed = i / totalElapsed
    println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - read all in ${xf(totalElapsed)}s")
  }

  final def sliceBytes(bytes: Array[Byte]) = {
    val slice = Array.ofDim[Byte](4)
    System.arraycopy(bytes, 0, slice, 0, 4)
    slice
  }

  final def intToBytes(i: Int) = ByteBuffer.allocate(4).putInt(i).array
  final def bytesToInt(bytes: Array[Byte]) = ByteBuffer.wrap(bytes).getInt
}


package khipu.tools

import java.io.File
import kesque.Kesque
import kesque.KesqueTable
import khipu.TKeyVal
import khipu.crypto
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags
import org.rocksdb.OptimisticTransactionDB
import org.rocksdb.Options
import scala.collection.mutable

/**
 * Fill memory:
 * # stress -m 1 --vm-bytes 25G --vm-keep
 */
object KesqueTool {
  def main(args: Array[String]) {
    val dbtool = new KesqueTool()

    dbtool.test(200000000)
  }
}
class KesqueTool() {
  private def xf(n: Double) = "%1$10.1f".format(n)
  val mapSize = 30 * 1024 * 1024 * 1024L
  val averDataSize = 1024

  val khipuPath = new File(classOf[KesqueTool].getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile.getParentFile
  val configDir = new File(khipuPath, "../src/main/resources")

  val configFile = new File(configDir, "kafka.server.properties")
  val props = org.apache.kafka.common.utils.Utils.loadProps(configFile.getAbsolutePath)
  val kesque = new Kesque(props)

  val home = {
    val h = new File("/home/dcaoyuan/tmp")
    if (!h.exists) {
      h.mkdirs()
    }
    println(s"index db home: $h")
    h
  }

  val topic = "ddtest"

  lazy val lmdbEnv = Env.create()
    .setMapSize(mapSize)
    .setMaxDbs(6)
    .open(home, EnvFlags.MDB_NOTLS, EnvFlags.MDB_NORDAHEAD, EnvFlags.MDB_NOSYNC, EnvFlags.MDB_NOMETASYNC)

  lazy val rocksdbPath = new File(home, topic)
  lazy val rocksdbOptions = new Options().setCreateIfMissing(true).setMaxOpenFiles(-1)
  lazy val rocksDbTable = OptimisticTransactionDB.open(rocksdbOptions, rocksdbPath.getAbsolutePath)

  def test(total: Int) = {
    val table = kesque.getKesqueTable(Array(topic), Right(rocksDbTable), fetchMaxBytes = 4096)

    val keys = write(table, total)
    read(table, keys)

    System.exit(0)
  }

  def write(table: KesqueTable, total: Int) = {
    val batchSize = 4000

    val keysToRead = new java.util.ArrayList[Array[Byte]]()
    val start0 = System.nanoTime
    var start = System.nanoTime
    var elapsed = 0L
    var totalElapsed = 0L
    var i = 0L
    val nKeysToRead = 1000000
    val keyInterval = math.max(total / nKeysToRead, 1)

    val kvs = new mutable.ArrayBuffer[TKeyVal]()
    while (i < total) {

      var j = 0
      while (j < batchSize && i < total) {
        val v = Array.ofDim[Byte](averDataSize)
        new scala.util.Random(System.currentTimeMillis).nextBytes(v)
        //val bs = ByteBuffer.allocate(8).putLong(i).array
        //System.arraycopy(bs, 0, v, v.length - bs.length, bs.length)

        val k = crypto.kec256(v)

        start = System.nanoTime

        kvs += TKeyVal(k, v, -1, 0)

        val duration = System.nanoTime - start
        elapsed += duration
        totalElapsed += duration

        if (i % keyInterval == 0) {
          keysToRead.add(k)
        }

        j += 1
        i += 1
      }

      start = System.nanoTime

      val n = table.write(kvs, topic)

      kvs.clear()

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

    keysToRead
  }

  def read(table: KesqueTable, keys: java.util.ArrayList[Array[Byte]]) {
    java.util.Collections.shuffle(keys)

    val start0 = System.nanoTime
    var start = System.nanoTime
    val itr = keys.iterator
    var i = 0
    while (itr.hasNext) {
      val k = itr.next

      table.read(k, topic) match {
        case Some(x) =>
        case None    => println(s"===> no data for ${khipu.toHexString(k)}")
      }

      if (i > 0 && i % 10000 == 0) {
        val elapsed = (System.nanoTime - start) / 1000000000.0 // sec
        val speed = 10000 / elapsed
        println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - 0x${khipu.Hash(k)}")
        start = System.nanoTime
      }

      i += 1
    }

    val totalElapsed = (System.nanoTime - start0) / 1000000000.0 // sec
    val speed = i / totalElapsed
    println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - read all in ${xf(totalElapsed)}s")
  }
}


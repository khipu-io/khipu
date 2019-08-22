package khipu.tools

import java.io.File
import java.nio.ByteBuffer
import khipu.Hash
import khipu.crypto
import org.rocksdb.OptimisticTransactionDB
import org.rocksdb.Options
import org.rocksdb.ReadOptions
import org.rocksdb.WriteOptions
import scala.collection.mutable

/**
 * Fill memory:
 * # stress -m 1 --vm-bytes 25G --vm-keep
 */
object RocksdbTool {
  def main(args: Array[String]) {
    val dbTool = new RocksdbTool()

    dbTool.test("table1", 5000000)
    System.exit(0)
  }
}
class RocksdbTool() {
  private def xf(n: Double) = "%1$10.1f".format(n)

  val averDataSize = 1024

  val home = {
    val h = new File("/home/dcaoyuan/tmp")
    if (!h.exists) {
      h.mkdirs()
    }
    println(s"rocksdb home: $h")
    h
  }

  val options = new Options().setCreateIfMissing(true).setMaxOpenFiles(-1)
  val writeOptions = new WriteOptions()
  val readOptions = new ReadOptions()

  def test(tableName: String, num: Int) = {
    val path = new File(home, tableName)
    val table = OptimisticTransactionDB.open(options, path.getAbsolutePath)

    val keys = write(table, num)
    read(table, keys)

    table.close()
  }

  def write(table: OptimisticTransactionDB, num: Int) = {
    val keysToRead = new java.util.ArrayList[Array[Byte]]()
    val start0 = System.nanoTime
    var start = System.nanoTime
    var elapsed = 0L
    var totalElapsed = 0L
    var i = 0
    val nKeysToRead = 1000000
    val keyInterval = math.max(num / nKeysToRead, 1)
    while (i < num) {

      var j = 0
      val txn = table.beginTransaction(writeOptions)
      while (j < 4000 && i < num) {
        val v = Array.ofDim[Byte](averDataSize)
                val bs = ByteBuffer.allocate(8).putLong(i).array
        System.arraycopy(bs, 0, v, v.length - bs.length, bs.length)
        
        val k = crypto.kec256(v)

        start = System.nanoTime

        try {
          txn.put(k, v)
        } catch {
          case ex: Throwable => println(ex)
        } finally {
        }

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

      try {
        txn.commit()
      } catch {
        case ex: Throwable =>
          txn.rollback()
          println(ex)
      } finally {
        txn.close()
      }

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

    val speed = i / (totalElapsed / 1000000000.0)
    println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - write all in ${xf((totalElapsed / 1000000000.0))}s")

    keysToRead
  }

  def read(table: OptimisticTransactionDB, keys: java.util.ArrayList[Array[Byte]]) {
    java.util.Collections.shuffle(keys)

    val start0 = System.nanoTime
    var start = System.nanoTime
    val itr = keys.iterator
    var i = 0
    while (itr.hasNext) {
      val k = itr.next
      val sKey = sliceBytes(k)

      try {
        val gotData = Option(table.get(readOptions, k))
        if (gotData.isEmpty) {
          println(s"===> no data for ${khipu.toHexString(sKey)} of ${khipu.toHexString(k)}")
        }
      } catch {
        case ex: Throwable =>
          println(ex)
          null
      }

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


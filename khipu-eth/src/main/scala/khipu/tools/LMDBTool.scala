package khipu.tools

import java.io.File
import java.nio.ByteBuffer
import khipu.Hash
import khipu.crypto
import scala.util.Random
import org.lmdbjava.EnvFlags
import org.lmdbjava.Env
import org.lmdbjava.Dbi
import org.lmdbjava.DbiFlags
import org.lmdbjava.GetOp
import org.lmdbjava.SeekOp
import scala.collection.mutable

/**
 * Fill memory:
 * # stress -m 1 --vm-bytes 25G --vm-keep
 */
object LMDBTool {
  def main(args: Array[String]) {
    val bdbTool = new LMDBTool()

    bdbTool.test(100000000)
  }
}
class LMDBTool() {
  private def xf(n: Double) = "%1$10.1f".format(n)

  val cacheSize = 1024 * 1024 * 1024L // 1G
  val bufLen = 1024 * 1024
  val mapSize = 100 * 1024 * 1024 * 1024L

  val averKeySize = 4
  val averDataSize = 64
  val hashNumElements = 300000000

  val home = new File("/home/dcaoyuan/tmp")

  val tableName = "storage"
  val tableDir = new File(home, tableName)
  if (!tableDir.exists) {
    tableDir.mkdirs()
  }

  val env = Env.create()
    .setMapSize(mapSize)
    .setMaxDbs(6)
    .open(tableDir, EnvFlags.MDB_NOLOCK, EnvFlags.MDB_NORDAHEAD)

  def test(num: Int) = {
    val table = env.openDbi(tableName, DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT)

    val keys = write(table, num)
    read(table, keys)

    table.close()
    env.close()
    System.exit(0)
  }

  def write(table: Dbi[ByteBuffer], num: Int) = {
    val keyBuf = ByteBuffer.allocateDirect(env.getMaxKeySize)
    val valBuf = ByteBuffer.allocateDirect(100 * 1024) // will grow when needed

    val keys = new java.util.ArrayList[Array[Byte]]()
    val start0 = System.nanoTime
    var start = System.nanoTime
    var elapsed = 0L
    var totalElapsed = 0L
    var i = 0
    val nKeysToRead = 1000000
    val keyInterval = math.max(num / nKeysToRead, 1)
    while (i < num) {

      var j = 0
      val txn = env.txnWrite()
      while (j < 4000 && i < num) {
        val v = Array.ofDim[Byte](64)
        Random.nextBytes(v)
        val k = crypto.kec256(v)

        start = System.nanoTime

        val sKey = sliceBytes(k)
        try {
          keyBuf.put(sKey).flip()
          valBuf.put(v).flip()
          if (!table.put(txn, keyBuf, valBuf)) {
            println(s"put failed: ${khipu.toHexString(sKey)}")
          }
        } catch {
          case ex: Throwable =>
            txn.abort()
            println(ex)
        } finally {
          keyBuf.clear()
          valBuf.clear()
        }

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

      try {
        txn.commit()
      } catch {
        case ex: Throwable =>
          txn.abort()
          println(ex)
      }

      val duration = System.nanoTime - start
      elapsed += duration
      totalElapsed += duration

      if (i % 100000 == 0) {
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

  def read(table: Dbi[ByteBuffer], keys: java.util.ArrayList[Array[Byte]]) {
    java.util.Collections.shuffle(keys)

    val keyBuf = ByteBuffer.allocateDirect(env.getMaxKeySize)

    val start0 = System.nanoTime
    var start = System.nanoTime
    val itr = keys.iterator
    var i = 0
    while (itr.hasNext) {
      val k = itr.next
      val sKey = sliceBytes(k)
      keyBuf.put(sKey).flip()

      val txn = env.txnRead()
      try {
        val cursor = table.openCursor(txn)
        cursor.get(keyBuf, GetOp.MDB_SET_KEY)

        var gotData: Option[Array[Byte]] = None
        if (cursor.get(keyBuf, GetOp.MDB_SET_KEY)) {
          val dataBytes = Array.ofDim[Byte](cursor.`val`.remaining)
          cursor.`val`.get(dataBytes)
          val fullKey = crypto.kec256(dataBytes)

          if (java.util.Arrays.equals(fullKey, k)) {
            gotData = Some(dataBytes)
          }

          while (gotData.isEmpty && cursor.seek(SeekOp.MDB_NEXT_DUP)) {
            val dataBytes = Array.ofDim[Byte](cursor.`val`.remaining)
            cursor.`val`.get(dataBytes)
            val fullKey = crypto.kec256(dataBytes)
            if (java.util.Arrays.equals(fullKey, k)) {
              gotData = Some(dataBytes)
            }
          }
        }

        cursor.close()

        if (gotData.isEmpty) {
          println(s"===> no data for ${khipu.toHexString(sKey)} of ${khipu.toHexString(k)}")
        }
      } catch {
        case ex: Throwable =>
          txn.abort()
          println(ex)
          null
      }
      txn.commit()

      if (i % 10000 == 0) {
        val elapsed = (System.nanoTime - start) / 1000000000.0 // sec
        val speed = 10000 / elapsed
        val hashKey = Hash(k)
        println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - 0x$hashKey")
        start = System.nanoTime
      }

      keyBuf.clear()
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


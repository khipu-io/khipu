package khipu.tools

import com.sleepycat.db.Database
import com.sleepycat.db.DatabaseConfig
import com.sleepycat.db.DatabaseEntry
import com.sleepycat.db.DatabaseException
import com.sleepycat.db.DatabaseType
import com.sleepycat.db.Environment
import com.sleepycat.db.EnvironmentConfig
import com.sleepycat.db.Hasher
import com.sleepycat.db.MultipleDataEntry
import com.sleepycat.db.OperationStatus
import java.io.File
import java.nio.ByteBuffer
import khipu.Hash
import khipu.crypto
import scala.util.Random
import scala.collection.mutable

object KhipuHasher extends Hasher {
  def hash(db: Database, data: Array[Byte], len: Int) = Hash.intHash(data)
}

object BDBTool {
  def main(args: Array[String]) {
    val bdbTool = new BDBTool(DatabaseType.BTREE, isTransactional = false)

    bdbTool.test(num = 100000000, isBulk = true)
  }
}
class BDBTool(databaseType: DatabaseType, isTransactional: Boolean) {
  private def xf(n: Double) = "%1$10.1f".format(n)

  val byteOrder = 4321 // big endian
  val cacheSize = 3 * 1024 * 1024 * 1024L // 3G
  val bufLen = 1024 * 1024

  val averKeySize = 4
  val averDataSize = 64
  val hashNumElements = 300000000

  val home = new File("/home/dcaoyuan/tmp")

  val tableName = "storage"
  val tableDir = new File(home, tableName)
  if (!tableDir.exists) {
    tableDir.mkdirs()
  }

  val envconf = new EnvironmentConfig()
  envconf.setAllowCreate(true)
  envconf.setInitializeCache(true) // must set otherwise "BDB0595 environment did not include a memory pool"
  envconf.setInitializeCDB(true)
  envconf.addDataDir(tableDir)
  envconf.setCacheSize(cacheSize)
  if (isTransactional) {
    envconf.setLogAutoRemove(true)
    envconf.setTransactional(true)
  }

  val dbenv = new Environment(home, envconf)

  val dbconf = new DatabaseConfig()
  dbconf.setAllowCreate(true)
  dbconf.setByteOrder(byteOrder)
  dbconf.setType(databaseType)
  dbconf.setSortedDuplicates(true)
  dbconf.setCreateDir(tableDir)
  if (isTransactional) {
    dbconf.setTransactional(true)
  }

  databaseType match {
    case DatabaseType.HASH =>
      dbconf.setHasher(KhipuHasher)
      dbconf.setHashNumElements(hashNumElements)
      val pageSize = 4096
      dbconf.setPageSize(pageSize)
      dbconf.setHashFillFactor((pageSize - 32) / (averKeySize + averDataSize + 8))

    case DatabaseType.BTREE =>
  }

  def test(num: Int, isBulk: Boolean) = {
    val table = dbenv.openDatabase(null, tableName + "1", null, dbconf)

    val keys = write(table, num, isBulk, isTransactional)
    read(table, keys)

    table.close()
    dbenv.close()
    System.exit(0)
  }

  def write(table: Database, num: Int, isBulk: Boolean, isTransactional: Boolean) = {
    val keys = new java.util.ArrayList[Array[Byte]]()
    val start0 = System.nanoTime
    var start = System.nanoTime
    var elapsed = 0L
    var totalElapsed = 0L
    var i = 0
    val nKeysToRead = 1000000
    val keyInterval = math.max(num / nKeysToRead, 1)
    while (i < num) {

      val (keySet, dataSet) = if (isBulk) {
        val keySet = new MultipleDataEntry()
        keySet.setData(Array.ofDim[Byte](bufLen))
        keySet.setUserBuffer(bufLen, true)

        val dataSet = new MultipleDataEntry()
        dataSet.setData(Array.ofDim[Byte](bufLen))
        dataSet.setUserBuffer(bufLen, true)

        (keySet, dataSet)
      } else {
        (null, null)
      }

      var j = 0
      val txn = if (isTransactional) dbenv.beginTransaction(null, null) else null
      while (j < 4000 && i < num) {
        val v = Array.ofDim[Byte](64)
        Random.nextBytes(v)
        val k = crypto.kec256(v)

        start = System.nanoTime

        val sKey = sliceBytes(k)
        if (isBulk) {
          keySet.append(sKey)
          dataSet.append(v)
        } else {
          try {
            table.put(txn, new DatabaseEntry(sKey), new DatabaseEntry(v)) match {
              case OperationStatus.SUCCESS =>
              case x                       => println(s"put failed: $x")
            }
          } catch {
            case ex: DatabaseException =>
              if (isTransactional) {
                txn.abort()
              }
              println(ex)
          }
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
        if (isBulk) {
          // use putMultiple instead of putMultipleKey for bulk insert _duplicate_ key/data pairs
          table.putMultiple(txn, keySet, dataSet, true)
        }
        if (isTransactional) {
          txn.commit()
        }
        table.sync() // flush to disk
      } catch {
        case ex: DatabaseException =>
          if (isTransactional) {
            txn.abort()
          }
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

  def read(table: Database, keys: java.util.ArrayList[Array[Byte]]) {
    java.util.Collections.shuffle(keys)

    val start0 = System.nanoTime
    var start = System.nanoTime
    val itr = keys.iterator
    var i = 0
    while (itr.hasNext) {
      val k = itr.next
      val shortKey = sliceBytes(k)
      val key = new DatabaseEntry(shortKey)

      val cursor = try {
        table.openCursor(null, null)
      } catch {
        case ex: DatabaseException =>
          println(ex)
          null
      }

      var data = new DatabaseEntry()
      var gotData: Option[Array[Byte]] = None
      if (cursor.getSearchKey(key, data, null) == OperationStatus.SUCCESS) {
        val dataBytes = data.getData
        val fullKey = crypto.kec256(dataBytes)

        if (java.util.Arrays.equals(fullKey, k)) {
          gotData = Some(dataBytes)
        }

        data = new DatabaseEntry()
        while (gotData.isEmpty && cursor.getNextDup(key, data, null) == OperationStatus.SUCCESS) {
          val dataBytes = data.getData
          val fullKey = crypto.kec256(dataBytes)
          if (java.util.Arrays.equals(fullKey, k)) {
            gotData = Some(dataBytes)
          }
          data = new DatabaseEntry()
        }
      }

      cursor.close()

      if (gotData.isEmpty) {
        println(s"===> no data for ${khipu.toHexString(shortKey)} of ${khipu.toHexString(k)}")
      }

      if (i % 10000 == 0) {
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


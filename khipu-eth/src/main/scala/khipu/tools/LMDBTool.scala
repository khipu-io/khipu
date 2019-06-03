package khipu.tools

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteOrder
import khipu.Hash
import khipu.crypto
import scala.util.Random
import org.lmdbjava.Env
import org.lmdbjava.EnvFlags
import org.lmdbjava.Dbi
import org.lmdbjava.DbiFlags
import org.lmdbjava.GetOp
import org.lmdbjava.PutFlags
import org.lmdbjava.SeekOp
import scala.collection.mutable

/**
 * Fill memory:
 * # stress -m 1 --vm-bytes 25G --vm-keep
 */
object LMDBTool {
  def main(args: Array[String]) {
    val dbTool = new LMDBTool()

    dbTool.test2("table1", 50000000)

    System.exit(0)
  }
}
class LMDBTool() {
  private def xf(n: Double) = "%1$10.1f".format(n)

  val mapSize = 100 * 1024 * 1024 * 1024L

  val COMPILED_MAX_KEY_SIZE = 511

  val IntIndexKey = true
  val IntTableKey = false

  val INDEX_KEY_SIZE = if (IntIndexKey) 4 else 8 // decide collisons probability
  val TABLE_KEY_SIZE = if (IntTableKey) 4 else 8 // decide max number of records

  val DATA_SIZE = 1024

  val home = {
    val h = new File("/home/dcaoyuan/tmp")
    if (!h.exists) {
      h.mkdirs()
    }
    println(s"lmdb home: $h")
    h
  }

  val env = Env.create()
    .setMapSize(mapSize)
    .setMaxDbs(6)
    .setMaxReaders(1024)
    .open(home, EnvFlags.MDB_NOTLS, EnvFlags.MDB_NORDAHEAD)

  def test1(tableName: String, num: Int) = {
    val table = if (DATA_SIZE > COMPILED_MAX_KEY_SIZE) {
      env.openDbi(tableName, DbiFlags.MDB_CREATE)
    } else {
      env.openDbi(tableName, DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT)
    }

    val keys = write1(table, num)
    read1(table, keys)

    table.close()
    closeEnv()
  }

  def write1(table: Dbi[ByteBuffer], num: Int) = {
    val keyBuf = ByteBuffer.allocateDirect(env.getMaxKeySize)
    val valBuf = ByteBuffer.allocateDirect(100 * 1024)

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
      val wtx = env.txnWrite()
      while (j < 4000 && i < num) {
        val v = Array.ofDim[Byte](DATA_SIZE)
        Random.nextBytes(v)
        val k = crypto.kec256(v)

        start = System.nanoTime

        val theKey = if (DATA_SIZE > COMPILED_MAX_KEY_SIZE) k else shortKey(k)
        try {
          keyBuf.put(theKey).flip()
          valBuf.put(v).flip()
          if (!table.put(wtx, keyBuf, valBuf)) {
            println(s"put failed: ${khipu.toHexString(theKey)}")
          }
        } catch {
          case ex: Throwable =>
            wtx.abort()
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
        wtx.commit()
      } catch {
        case ex: Throwable =>
          wtx.abort()
          println(ex)
      } finally {
        wtx.close()
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

    keys
  }

  def read1(table: Dbi[ByteBuffer], keys: java.util.ArrayList[Array[Byte]]) {
    java.util.Collections.shuffle(keys)

    val keyBuf = ByteBuffer.allocateDirect(env.getMaxKeySize)

    val start0 = System.nanoTime
    var start = System.nanoTime
    val itr = keys.iterator
    var i = 0
    while (itr.hasNext) {
      val k = itr.next
      val theKey = if (DATA_SIZE > COMPILED_MAX_KEY_SIZE) k else shortKey(k)
      keyBuf.put(theKey).flip()

      val rtx = env.txnRead()
      try {
        val cursor = table.openCursor(rtx)

        var gotData: Option[Array[Byte]] = None
        if (cursor.get(keyBuf, GetOp.MDB_SET_KEY)) {
          val data = Array.ofDim[Byte](cursor.`val`.remaining)
          cursor.`val`.get(data)
          val fullKey = crypto.kec256(data)
          if (java.util.Arrays.equals(fullKey, k)) {
            gotData = Some(data)
          }

          while (gotData.isEmpty && cursor.seek(SeekOp.MDB_NEXT_DUP)) {
            val data = Array.ofDim[Byte](cursor.`val`.remaining)
            cursor.`val`.get(data)
            val fullKey = crypto.kec256(data)
            if (java.util.Arrays.equals(fullKey, k)) {
              gotData = Some(data)
            }
          }
        }

        cursor.close()

        if (gotData.isEmpty) {
          println(s"===> no data for ${khipu.toHexString(theKey)} of ${khipu.toHexString(k)}")
        }
      } catch {
        case ex: Throwable =>
          rtx.abort()
          println(ex)
          null
      }
      rtx.commit()
      rtx.close()

      if (i > 0 && i % 10000 == 0) {
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

  def test2(tableName: String, num: Int) = {
    val table = env.openDbi(
      tableName,
      DbiFlags.MDB_CREATE,
      DbiFlags.MDB_INTEGERKEY
    )

    val index = env.openDbi(
      tableName + "_idx",
      DbiFlags.MDB_CREATE,
      DbiFlags.MDB_INTEGERKEY,
      DbiFlags.MDB_INTEGERDUP,
      DbiFlags.MDB_DUPSORT,
      DbiFlags.MDB_DUPFIXED
    )

    val txn = env.txnRead()
    val stat = table.stat(txn)
    val count = stat.entries
    println(s"number of existed records: $count ")
    txn.commit()
    txn.close()

    val keys = write2(table, index, count, num)
    read2(table, index, keys)

    table.close()
    index.close()
    closeEnv()
  }

  def write2(table: Dbi[ByteBuffer], index: Dbi[ByteBuffer], startId: Long, num: Int) = {
    val indexKey = ByteBuffer.allocateDirect(INDEX_KEY_SIZE)
    val indexVal = ByteBuffer.allocateDirect(TABLE_KEY_SIZE).order(ByteOrder.nativeOrder)
    val tableKey = ByteBuffer.allocateDirect(TABLE_KEY_SIZE).order(ByteOrder.nativeOrder)
    val tableVal = ByteBuffer.allocateDirect(DATA_SIZE)

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
      val wtx = env.txnWrite()
      while (j < 4000 && i < num) {
        val v = Array.ofDim[Byte](DATA_SIZE)
        Random.nextBytes(v)
        val k = crypto.kec256(v)

        start = System.nanoTime

        try {
          val id = i + startId
          indexKey.put(shortKey(k)).flip()
          if (IntTableKey) {
            indexVal.putInt(id.toInt).flip()
            tableKey.putInt(id.toInt).flip()
          } else {
            indexVal.putLong(id).flip()
            tableKey.putLong(id).flip()
          }
          tableVal.put(v).flip()

          if (!table.put(wtx, tableKey, tableVal, PutFlags.MDB_APPEND)) {
            println(s"table put failed: id $id, ${khipu.toHexString(k)}")
          }
          if (!index.put(wtx, indexKey, indexVal)) {
            println(s"index put failed: id $id, ${khipu.toHexString(k)}")
          }
        } catch {
          case ex: Throwable =>
            wtx.abort()
            ex.printStackTrace()
        } finally {
          tableKey.clear()
          tableVal.clear()
          indexKey.clear()
          indexVal.clear()
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
        wtx.commit()
      } catch {
        case ex: Throwable =>
          wtx.abort()
          ex.printStackTrace()
      } finally {
        wtx.close()
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

    keys
  }

  def read2(table: Dbi[ByteBuffer], index: Dbi[ByteBuffer], keys: java.util.ArrayList[Array[Byte]]) {
    java.util.Collections.shuffle(keys)

    val indexKey = ByteBuffer.allocateDirect(INDEX_KEY_SIZE)
    val tableKey = ByteBuffer.allocateDirect(TABLE_KEY_SIZE)

    val start0 = System.nanoTime
    var start = System.nanoTime
    val itr = keys.iterator
    var i = 0
    while (itr.hasNext) {
      val k = itr.next
      indexKey.put(shortKey(k)).flip()

      val rtx = env.txnRead()
      try {
        val indexCursor = index.openCursor(rtx)

        var gotData: Option[Array[Byte]] = None
        if (indexCursor.get(indexKey, GetOp.MDB_SET_KEY)) {
          val id = Array.ofDim[Byte](indexCursor.`val`.remaining)
          indexCursor.`val`.get(id)

          tableKey.put(id).flip()
          val tableVal = table.get(rtx, tableKey)
          if (tableVal ne null) {
            val data = Array.ofDim[Byte](tableVal.remaining)
            tableVal.get(data)
            val fullKey = crypto.kec256(data)
            if (java.util.Arrays.equals(fullKey, k)) {
              gotData = Some(data)
            }
          }

          while (gotData.isEmpty && indexCursor.seek(SeekOp.MDB_NEXT_DUP)) {
            val id = Array.ofDim[Byte](indexCursor.`val`.remaining)
            indexCursor.`val`.get(id)
            tableKey.clear().asInstanceOf[ByteBuffer].put(id).flip()
            val tableVal = table.get(rtx, tableKey)
            if (tableVal ne null) {
              val data = Array.ofDim[Byte](tableVal.remaining)
              tableVal.get(data)
              val fullKey = crypto.kec256(data)
              if (java.util.Arrays.equals(fullKey, k)) {
                gotData = Some(data)
              }
            }
          }
        } else {
          println(s"no index data got for ${khipu.toHexString(shortKey(k))}")
        }

        indexCursor.close()

        rtx.commit()

        if (gotData.isEmpty) {
          println(s"===> no data for ${khipu.toHexString(k)}")
        }

      } catch {
        case ex: Throwable =>
          rtx.abort()
          ex.printStackTrace()
          null
      }
      rtx.close()

      if (i > 0 && i % 10000 == 0) {
        val elapsed = (System.nanoTime - start) / 1000000000.0 // sec
        val speed = 10000 / elapsed
        val hashKey = Hash(k)
        println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - 0x$hashKey")
        start = System.nanoTime
      }

      indexKey.clear()
      tableKey.clear()
      i += 1
    }

    val totalElapsed = (System.nanoTime - start0) / 1000000000.0 // sec
    val speed = i / totalElapsed
    println(s"${java.time.LocalTime.now} $i ${xf(speed)}/s - read all in ${xf(totalElapsed)}s")
  }

  def test3(tableName: String) = {
    val table = env.openDbi(
      tableName,
      DbiFlags.MDB_CREATE,
      DbiFlags.MDB_INTEGERKEY
    )

    val txn = env.txnRead()
    val stat = table.stat(txn)
    val count = stat.entries
    println(s"number of existed records: $count ")
    txn.commit()
    txn.close()

    val tableKey = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder)
    val tableVal = ByteBuffer.allocateDirect(DATA_SIZE)

    var wtx = env.txnWrite()
    var i = 0
    while (i < 1010) {
      val v = Array.ofDim[Byte](DATA_SIZE)
      Random.nextBytes(v)
      tableKey.putLong(i).flip()
      tableVal.put(v).flip()
      if (!table.put(wtx, tableKey, tableVal, PutFlags.MDB_APPEND)) {
        println(s"table put failed: $i")
      }
      tableKey.clear()
      tableVal.clear()
      i += 1
    }
    wtx.commit()
    wtx.close()

    // delete 1000 ~ 1009
    wtx = env.txnWrite()
    i = 1000
    while (i < 1010) {
      tableKey.putLong(i).flip()
      table.delete(wtx, tableKey)
      i += 1
    }
    wtx.commit()
    wtx.close()

    // append 1000 ~ 1009 again with PutFlags.MDB_APPEND, should work
    wtx = env.txnWrite()
    i = 1000
    while (i < 1010) {
      tableKey.putLong(i).flip()
      tableVal.put(i.toString.getBytes).flip()
      table.put(wtx, tableKey, tableVal, PutFlags.MDB_APPEND)
      i += 1
    }
    wtx.commit()
    wtx.close()

    // read 1000 ~ 1009
    val rtx = env.txnRead()
    i = 1000
    while (i < 1010) {
      tableKey.putLong(i).flip()
      val tableVal = table.get(rtx, tableKey)
      if (tableVal ne null) {
        val data = Array.ofDim[Byte](tableVal.remaining)
        tableVal.get(data)
        println(s"$i: ${new String(data)}")
      } else {
        println(s"$i: null")
      }
      i += 1
    }
    rtx.commit()
    rtx.close()

    table.close()
    closeEnv()
  }

  private def shortKey(bytes: Array[Byte]) = {
    val slice = Array.ofDim[Byte](INDEX_KEY_SIZE)
    System.arraycopy(bytes, 0, slice, 0, INDEX_KEY_SIZE)
    slice
  }

  def closeEnv() {
    env.sync(true)
    env.close()
  }
}


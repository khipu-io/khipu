package khipu.storage.datasource

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import java.nio.ByteOrder
import khipu.Hash
import khipu.crypto
import khipu.util.Clock
import khipu.util.DirectByteBufferPool
import khipu.util.FIFOCache
import org.lmdbjava.Cursor
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.GetOp
import org.lmdbjava.PutFlags
import org.lmdbjava.SeekOp
import org.lmdbjava.Txn

object LmdbNodeDataSource {
  private val INDEX_KEY_SIZE = 4 // int
  private val TABLE_KEY_SIZE = 8 // long
}
final class LmdbNodeDataSource(
    val topic: String,
    env:       Env[ByteBuffer],
    cacheSize: Int
)(implicit system: ActorSystem) extends BlockDataSource[Hash, Array[Byte]] {
  type This = LmdbNodeDataSource

  import LmdbNodeDataSource._

  private val log = Logging(system, this.getClass)
  private val keyPool = DirectByteBufferPool.KeyPool

  private val cache = new FIFOCache[Hash, Array[Byte]](cacheSize)

  // LMDB defines compile-time constant MDB_MAXKEYSIZE=511 bytes
  // Key sizes must be between 1 and mdb_env_get_maxkeysize() inclusive. 
  // The same applies to data sizes in databases with the MDB_DUPSORT flag. 
  // Other data items can in theory be from 0 to 0xffffffff bytes long. 
  private val table = env.openDbi(
    topic,
    DbiFlags.MDB_CREATE,
    DbiFlags.MDB_INTEGERKEY
  )

  private val index = env.openDbi(
    topic + "_idx",
    DbiFlags.MDB_CREATE,
    DbiFlags.MDB_INTEGERKEY,
    DbiFlags.MDB_INTEGERDUP,
    DbiFlags.MDB_DUPSORT,
    DbiFlags.MDB_DUPFIXED
  )

  val clock = new Clock()

  private var nextId: Long = count

  log.info(s"Table $topic nodes $count")

  def get(key: Hash): Option[Array[Byte]] = {

    cache.get(key) match {
      case None =>
        val start = System.nanoTime

        var keyBufs: List[ByteBuffer] = Nil
        var ret: Option[Array[Byte]] = None
        var rtx: Txn[ByteBuffer] = null
        var indexCursor: Cursor[ByteBuffer] = null
        try {
          rtx = env.txnRead()

          indexCursor = index.openCursor(rtx)

          val indexKey = keyPool.acquire()
          val tableKey = keyPool.acquire()

          indexKey.put(shortKey(key.bytes)).flip()
          if (indexCursor.get(indexKey, GetOp.MDB_SET_KEY)) {
            val id = Array.ofDim[Byte](indexCursor.`val`.remaining)
            indexCursor.`val`.get(id)
            tableKey.put(id).flip()
            val tableVal = table.get(rtx, tableKey)
            if (tableVal ne null) {
              val data = Array.ofDim[Byte](tableVal.remaining)
              tableVal.get(data)
              val fullKey = crypto.kec256(data)

              log.debug(s"get1 $key -> ${shortKey(key.bytes).mkString(",")} -> $id -> ${Hash(crypto.kec256(data))} -> ${Hash(fullKey)}")

              if (java.util.Arrays.equals(fullKey, key.bytes)) {
                ret = Some(data)
              }
            }

            while (ret.isEmpty && indexCursor.seek(SeekOp.MDB_NEXT_DUP)) {
              tableKey.clear()

              val id = Array.ofDim[Byte](indexCursor.`val`.remaining)
              indexCursor.`val`.get(id)
              tableKey.put(id).flip()
              val tableVal = table.get(rtx, tableKey)
              if (tableVal ne null) {
                val data = Array.ofDim[Byte](tableVal.remaining)
                tableVal.get(data)
                val fullKey = crypto.kec256(data)

                log.debug(s"get2 $key -> ${shortKey(key.bytes).mkString(",")} -> $id -> ${Hash(crypto.kec256(data))} -> ${Hash(fullKey)}")

                if (java.util.Arrays.equals(fullKey, key.bytes)) {
                  ret = Some(data)
                }
              }
            }
          }

          keyBufs ::= indexKey
          keyBufs ::= tableKey
          rtx.commit()
        } catch {
          case ex: Throwable =>
            if (rtx ne null) {
              rtx.abort()
            }
            log.error(ex, ex.getMessage)
        } finally {
          if (indexCursor ne null) {
            indexCursor.close()
          }
          if (rtx ne null) {
            rtx.close()
          }

          keyBufs foreach keyPool.release
        }

        clock.elapse(System.nanoTime - start)

        ret foreach { data => cache.put(key, data) }
        ret

      case x => x
    }
  }

  def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): LmdbNodeDataSource = {
    // TODO what's the meaning of remove a node? sometimes causes node not found

    var byteBufs: List[ByteBuffer] = Nil
    var wtx: Txn[ByteBuffer] = null
    try {
      wtx = env.txnWrite()

      val (newNextId, bufs) = toUpsert.foldLeft(nextId, List[ByteBuffer]()) {
        case ((id, bufs), (key, data)) =>
          log.debug(s"put $key -> ${shortKey(key.bytes).mkString(",")} -> $id -> ${Hash(crypto.kec256(data))}")

          val sKey = shortKey(key.bytes)
          val indexKey = keyPool.acquire()
          val indexVal = keyPool.acquire().order(ByteOrder.nativeOrder)
          val tableKey = keyPool.acquire().order(ByteOrder.nativeOrder)
          val tableVal = ByteBuffer.allocateDirect(data.length)

          indexKey.put(sKey).flip()
          indexVal.putLong(id).flip()
          tableKey.putLong(id).flip()
          tableVal.put(data).flip()

          index.put(wtx, indexKey, indexVal)
          table.put(wtx, tableKey, tableVal, PutFlags.MDB_APPEND)

          (id + 1, indexKey :: indexVal :: tableKey :: bufs)
      }

      byteBufs = bufs
      wtx.commit()

      nextId = newNextId

      toUpsert foreach {
        case (key, data) => cache.put(key, data)
      }
    } catch {
      case ex: Throwable =>
        if (wtx ne null) {
          wtx.abort()
        }
        log.error(ex, s"$topic ${ex.getMessage}")
    } finally {
      if (wtx ne null) {
        wtx.close()
      }
      byteBufs foreach keyPool.release
    }

    this
  }

  def count = {
    val rtx = env.txnRead()
    val stat = table.stat(rtx)
    val ret = stat.entries
    rtx.commit()
    rtx.close()
    ret
  }

  def cacheHitRate = cache.hitRate
  def cacheReadCount = cache.readCount
  def resetCacheHitRate() = cache.resetHitRate()

  private def shortKey(bytes: Array[Byte]) = {
    val slice = Array.ofDim[Byte](INDEX_KEY_SIZE)
    System.arraycopy(bytes, 0, slice, 0, INDEX_KEY_SIZE)
    slice
  }

  def stop() {
    // not necessary to close db, we'll call env.sync(true) to force sync 
  }
}

package khipu.store.datasource

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import java.nio.ByteOrder
import kesque.FIFOCache
import kesque.TVal
import khipu.Hash
import khipu.crypto
import khipu.util.Clock
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
    cacheSize: Int             = 10000
)(implicit system: ActorSystem) extends NodeDataSource {
  type This = LmdbNodeDataSource

  import LmdbNodeDataSource._

  private val log = Logging(system, this.getClass)

  private val cache = new FIFOCache[Hash, TVal](cacheSize)

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

  private val indexKey = ByteBuffer.allocateDirect(INDEX_KEY_SIZE)
  private val indexVal = ByteBuffer.allocateDirect(TABLE_KEY_SIZE).order(ByteOrder.nativeOrder)
  private val tableKey = ByteBuffer.allocateDirect(TABLE_KEY_SIZE).order(ByteOrder.nativeOrder)
  private var tableVal = ByteBuffer.allocateDirect(100 * 1024) // will grow when needed

  val clock = new Clock()

  private var _currId: Long = {
    val rtx = env.txnRead()
    val stat = table.stat(rtx)
    val ret = stat.entries
    log.info(s"Table $topic last id is $ret")
    rtx.commit()
    rtx.close()
    ret
  }
  private def currId = _currId
  def currId_=(_currId: Long) {
    this._currId = _currId
  }

  def get(key: Hash): Option[TVal] = {
    val indexKey = ByteBuffer.allocateDirect(INDEX_KEY_SIZE)
    val tableKey = ByteBuffer.allocateDirect(TABLE_KEY_SIZE)

    cache.get(key) match {
      case None =>
        val start = System.nanoTime

        var ret: Option[Array[Byte]] = None
        var rtx: Txn[ByteBuffer] = null
        var indexCursor: Cursor[ByteBuffer] = null
        try {
          rtx = env.txnRead()

          indexCursor = index.openCursor(rtx)
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
        }

        clock.elapse(System.nanoTime - start)

        ret.map(TVal(_, -1, -1))

      case x => x
    }
  }

  def update(toRemove: Set[Hash], toUpsert: Map[Hash, TVal]): LmdbNodeDataSource = {
    // TODO what's the meaning of remove a node? sometimes causes node not found
    //table.remove(toRemove.map(_.bytes).toList)

    var id = currId
    var wtx: Txn[ByteBuffer] = null
    try {
      wtx = env.txnWrite()

      toUpsert foreach {
        case (key, tval @ TVal(data, _, _)) =>
          id += 1

          log.debug(s"put $key -> ${shortKey(key.bytes).mkString(",")} -> $id -> ${Hash(crypto.kec256(data))}")

          indexKey.put(shortKey(key.bytes)).flip()
          indexVal.putLong(id).flip()
          tableKey.putLong(id).flip()

          ensureValueBufferSize(data.length)
          tableVal.put(data).flip()

          cache.put(key, tval)

          index.put(wtx, indexKey, indexVal)
          table.put(wtx, tableKey, tableVal, PutFlags.MDB_APPEND)

          indexKey.clear()
          indexVal.clear()
          tableKey.clear()
          tableVal.clear()
      }

      wtx.commit()
      currId = id
    } catch {
      case ex: Throwable =>
        if (wtx ne null) {
          wtx.abort()
        }
        log.error(ex, s"{ex.getMessage} at id: $id")
    } finally {
      if (wtx ne null) {
        wtx.close()
      }
      indexKey.clear()
      indexVal.clear()
      tableKey.clear()
      tableVal.clear()
    }

    this
  }

  def cacheHitRate = cache.hitRate
  def cacheReadCount = cache.readCount
  def resetCacheHitRate() = cache.resetHitRate()

  private def shortKey(bytes: Array[Byte]) = {
    val slice = Array.ofDim[Byte](INDEX_KEY_SIZE)
    System.arraycopy(bytes, 0, slice, 0, INDEX_KEY_SIZE)
    slice
  }

  private def ensureValueBufferSize(size: Int): Unit = {
    if (tableVal.remaining < size) {
      tableVal = ByteBuffer.allocateDirect(size * 2)
    }
  }

  def close() {
    index.close()
    table.close()
  }
}

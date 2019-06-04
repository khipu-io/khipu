package khipu.store.datasource

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.locks.ReentrantReadWriteLock
import kesque.FIFOCache
import kesque.TVal
import khipu.Hash
import khipu.crypto
import khipu.util.Clock
import org.lmdbjava.DbiFlags
import org.lmdbjava.Env
import org.lmdbjava.Txn

object LmdbBlockDataSource {
  private var timestampToKey = Array.ofDim[Array[Byte]](200)
  private var keyToTimestamp = Map[Hash, Long]()

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  def getTimestampByKey(key: Hash): Option[Long] = {
    keyToTimestamp.get(key)
  }

  def getKeyByTimestamp(timestamp: Long): Option[Hash] = {
    try {
      readLock.lock()

      if (timestamp >= 0 && timestamp < timestampToKey.length) {
        Option(timestampToKey(timestamp.toInt)).map(Hash(_))
      } else {
        None
      }
    } finally {
      readLock.unlock()
    }
  }

  def putTimestampToKey(timestamp: Long, key: Hash) {
    try {
      writeLock.lock()

      if (timestamp > timestampToKey.length - 1) {
        val newArr = Array.ofDim[Array[Byte]]((timestamp * 1.2).toInt)
        System.arraycopy(timestampToKey, 0, newArr, 0, timestampToKey.length)
        timestampToKey = newArr
      }
      timestampToKey(timestamp.toInt) = key.bytes
      keyToTimestamp += (key -> timestamp)
    } finally {
      writeLock.unlock()
    }
  }

  def removeTimestamp(key: Hash) {
    keyToTimestamp.get(key) foreach { timestamp => timestampToKey(timestamp.toInt) = null }
    keyToTimestamp -= key
  }
}
final class LmdbBlockDataSource(
    val topic: String,
    val env:   Env[ByteBuffer],
    cacheSize: Int             = 10000
)(implicit system: ActorSystem) extends BlockDataSource {
  type This = LmdbBlockDataSource

  private val KEY_SIZE = 8 // long - blocknumber

  private val log = Logging(system, this.getClass)

  private val cache = new FIFOCache[Long, TVal](cacheSize)

  val table = env.openDbi(
    topic,
    DbiFlags.MDB_CREATE,
    DbiFlags.MDB_INTEGERKEY
  )

  private val tableKey = ByteBuffer.allocateDirect(KEY_SIZE).order(ByteOrder.nativeOrder)
  private var tableVal = ByteBuffer.allocateDirect(100 * 1024) // will grow when needed

  val clock = new Clock()

  def get(key: Long): Option[TVal] = {
    val tableKey = ByteBuffer.allocateDirect(KEY_SIZE).order(ByteOrder.nativeOrder)

    cache.get(key) match {
      case None =>
        val start = System.nanoTime
        tableKey.putLong(key).flip()

        var ret: Option[Array[Byte]] = None
        var txn: Txn[ByteBuffer] = null
        try {
          txn = env.txnRead()

          val tableVal = table.get(txn, tableKey)
          if (tableVal ne null) {
            val data = Array.ofDim[Byte](tableVal.remaining)
            tableVal.get(data)
            ret = Some(data)
          }

          txn.commit()
        } catch {
          case ex: Throwable =>
            if (txn ne null) {
              txn.abort()
            }
            log.error(ex, ex.getMessage)
        } finally {
          if (txn ne null) {
            txn.close()
          }
        }

        clock.elapse(System.nanoTime - start)

        ret.map(TVal(_, -1, key))

      case x => x
    }
  }

  def update(toRemove: Set[Long], toUpsert: Map[Long, TVal]): LmdbBlockDataSource = {
    // TODO what's the meaning of remove a node? sometimes causes node not found
    //table.remove(toRemove.map(_.bytes).toList)

    var wxn: Txn[ByteBuffer] = null
    try {
      wxn = env.txnWrite()
      toUpsert foreach {
        case (key, tval @ TVal(data, _, _)) =>
          cache.put(key, tval)

          tableKey.putLong(key).flip()

          ensureValueBufferSize(data.length)
          tableVal.put(data).flip()

          table.put(wxn, tableKey, tableVal)
          tableKey.clear()
          tableVal.clear()
      }
      wxn.commit()
    } catch {
      case ex: Throwable =>
        if (wxn ne null) wxn.abort()
        log.error(ex, ex.getMessage)
    } finally {
      if (wxn ne null) wxn.close()
    }

    this
  }

  def cacheHitRate = cache.hitRate
  def cacheReadCount = cache.readCount
  def resetCacheHitRate() = cache.resetHitRate()

  private def ensureValueBufferSize(size: Int): Unit = {
    if (tableVal.remaining < size) {
      tableVal = ByteBuffer.allocateDirect(size * 2)
    }
  }

  def close() = table.close()
}

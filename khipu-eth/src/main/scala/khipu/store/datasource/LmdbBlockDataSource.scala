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
import scala.collection.mutable

object LmdbBlockDataSource {
  private var timestampToKey = Array.ofDim[Array[Byte]](200)
  private val keyToTimestamp = new mutable.HashMap[Hash, Long]()

  val KEY_SIZE = 8 // long - blocknumber

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  def getTimestampByKey(key: Hash): Option[Long] = {
    try {
      readLock.lock()

      keyToTimestamp.get(key)
    } finally {
      readLock.unlock()
    }
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

  def getKeysByTimestamp(from: Long, to: Long): (Long, List[Hash]) = {
    try {
      readLock.lock()

      val ret = new mutable.ListBuffer[Hash]()
      var lastNumber = from
      var i = from
      while (i <= to && i < timestampToKey.length) {
        val key = timestampToKey(i.toInt)
        if (key ne null) {
          ret += Hash(key)
          lastNumber = i
        }
        i += 1
      }

      (lastNumber, ret.toList)
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

  private val log = Logging(system, this.getClass)
  private val keyPool = DirectByteBufferPool.KeyPool

  private val cache = new FIFOCache[Long, TVal](cacheSize)

  val table = env.openDbi(
    topic,
    DbiFlags.MDB_CREATE,
    DbiFlags.MDB_INTEGERKEY
  )

  val clock = new Clock()

  def get(key: Long): Option[TVal] = {
    cache.get(key) match {
      case None =>
        val start = System.nanoTime

        var keyBufs: List[ByteBuffer] = Nil
        var ret: Option[Array[Byte]] = None
        var txn: Txn[ByteBuffer] = null
        try {
          txn = env.txnRead()

          val tableKey = keyPool.acquire().order(ByteOrder.nativeOrder).putLong(key).flip().asInstanceOf[ByteBuffer]
          val tableVal = table.get(txn, tableKey)
          if (tableVal ne null) {
            val data = Array.ofDim[Byte](tableVal.remaining)
            tableVal.get(data)
            ret = Some(data)
          }

          keyBufs ::= tableKey
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

          keyBufs foreach keyPool.release
        }

        clock.elapse(System.nanoTime - start)

        ret.map(TVal(_, -1, key))

      case x => x
    }
  }

  def update(toRemove: Set[Long], toUpsert: Map[Long, TVal]): LmdbBlockDataSource = {
    // TODO what's the meaning of remove a node? sometimes causes node not found
    //table.remove(toRemove.map(_.bytes).toList)

    var keyBufs: List[ByteBuffer] = Nil
    var wxn: Txn[ByteBuffer] = null
    try {
      wxn = env.txnWrite()

      toUpsert foreach {
        case (key, tval @ TVal(data, _, _)) =>
          cache.put(key, tval)

          val tableKey = keyPool.acquire().order(ByteOrder.nativeOrder).putLong(key).flip().asInstanceOf[ByteBuffer]
          val tableVal = ByteBuffer.allocateDirect(data.length).put(data).flip().asInstanceOf[ByteBuffer]
          table.put(wxn, tableKey, tableVal)

          keyBufs ::= tableKey
      }

      wxn.commit()
    } catch {
      case ex: Throwable =>
        if (wxn ne null) wxn.abort()
        log.error(ex, ex.getMessage)
    } finally {
      if (wxn ne null) wxn.close()
      keyBufs foreach keyPool.release
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

  def close() = table.close()
}

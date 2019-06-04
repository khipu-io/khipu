package khipu.store.datasource

import akka.actor.ActorSystem
import akka.event.Logging
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import kesque.FIFOCache
import kesque.TVal
import khipu.Hash
import khipu.crypto
import khipu.util.Clock
import org.lmdbjava.Cursor
import org.lmdbjava.Dbi
import org.lmdbjava.Env
import org.lmdbjava.GetOp
import org.lmdbjava.SeekOp
import org.lmdbjava.Txn

final class PlainLmdbHeavyDataSource(
    val topic:         String,
    env:               Env[ByteBuffer],
    table:             Dbi[ByteBuffer],
    cacheSize:         Int             = 10000,
    isKeyTheValueHash: Boolean         = false,
    isWithTimestamp:   Boolean         = false
)(implicit system: ActorSystem) extends HeavyDataSource {
  type This = PlainLmdbHeavyDataSource

  private val log = Logging(system, this.getClass)

  private val cache = new FIFOCache[Hash, TVal](cacheSize)

  private val keyWriteBuf = ByteBuffer.allocateDirect(env.getMaxKeySize)
  private var dataWriteBuf = ByteBuffer.allocateDirect(100 * 1024) // will grow when needed

  private var timeIndex = if (isWithTimestamp) Array.ofDim[Array[Byte]](200) else Array.ofDim[Array[Byte]](0)
  private var keyToTimestamp = if (isWithTimestamp) Map[Hash, Long]() else Map[Hash, Long]()

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  val clock = new Clock()

  if (isWithTimestamp) {
    loadTimeIndex()
  }

  private def loadTimeIndex() {
    val start = System.nanoTime
    val txn = env.txnRead()
    val itr = table.iterate(txn)
    while (itr.hasNext) {
      val entry = itr.next()
      val key = new Array[Byte](entry.key.remaining)
      entry.key.get(key)

      val timestamp = entry.`val`.getLong()
      putTimestampToKey(timestamp, Hash(key))
    }
    itr.close()
    txn.commit()
    txn.close()
  }

  private var _currWritingTimestamp: Long = _
  private def writingTimestamp = _currWritingTimestamp
  def setWritingTimestamp(writingTimestamp: Long) {
    this._currWritingTimestamp = writingTimestamp
  }

  def getTimestampByKey(key: Hash): Option[Long] = {
    keyToTimestamp.get(key)
  }

  def getKeyByTimestamp(timestamp: Long): Option[Hash] = {
    try {
      readLock.lock()

      if (!isWithTimestamp) {
        None
      } else {
        if (timestamp >= 0 && timestamp < timeIndex.length) {
          Option(timeIndex(timestamp.toInt)).map(Hash(_))
        } else {
          None
        }
      }
    } finally {
      readLock.unlock()
    }
  }

  def putTimestampToKey(timestamp: Long, key: Hash) {
    try {
      writeLock.lock()

      if (timestamp > timeIndex.length - 1) {
        val newArr = Array.ofDim[Array[Byte]]((timestamp * 1.2).toInt)
        System.arraycopy(timeIndex, 0, newArr, 0, timeIndex.length)
        timeIndex = newArr
      }
      timeIndex(timestamp.toInt) = key.bytes
      keyToTimestamp += (key -> timestamp)
    } finally {
      writeLock.unlock()
    }
  }

  def get(key: Hash): Option[TVal] = {
    val keyReadBuf = ByteBuffer.allocateDirect(env.getMaxKeySize)

    cache.get(key) match {
      case None =>
        val start = System.nanoTime
        if (isKeyTheValueHash) {
          val sKey = sliceBytes(key.bytes)
          keyReadBuf.put(sKey).flip()
        } else {
          keyReadBuf.put(key.bytes).flip()
        }

        var txn: Txn[ByteBuffer] = null
        var cursor: Cursor[ByteBuffer] = null
        var gotData: Option[Array[Byte]] = None
        try {
          txn = env.txnRead()

          cursor = table.openCursor(txn)
          if (cursor.get(keyReadBuf, GetOp.MDB_SET_KEY)) {
            val data = Array.ofDim[Byte](cursor.`val`.remaining)
            cursor.`val`.get(data)
            if (isKeyTheValueHash) {
              val fullKey = crypto.kec256(data)
              if (java.util.Arrays.equals(fullKey, key.bytes)) {
                gotData = Some(data)
              }
            } else {
              gotData = Some(data)
            }

            if (isKeyTheValueHash) { // duplicate values should only happen in case of short key
              while (gotData.isEmpty && cursor.seek(SeekOp.MDB_NEXT_DUP)) {
                val data = Array.ofDim[Byte](cursor.`val`.remaining)
                cursor.`val`.get(data)
                val fullKey = crypto.kec256(data)
                if (java.util.Arrays.equals(fullKey, key.bytes)) {
                  gotData = Some(data)
                }
              }
            }
          }

          keyReadBuf.clear()

          txn.commit()
        } catch {
          case ex: Throwable =>
            if (txn ne null) txn.abort()
            log.error(ex, ex.getMessage)
        } finally {
          if (cursor ne null) cursor.close()
          if (txn ne null) txn.close()
        }

        clock.elapse(System.nanoTime - start)

        gotData map { data =>
          if (isWithTimestamp) {
            val buf = ByteBuffer.allocate(data.length)
            buf.put(data).flip()
            val timestamp = buf.getLong()
            val value = Array.ofDim[Byte](buf.remaining)
            buf.get(value)
            TVal(value, -1, timestamp)
          } else {
            TVal(data, -1, -1)
          }
        }

      case x => x
    }
  }

  def update(toRemove: Set[Hash], toUpsert: Map[Hash, TVal]): PlainLmdbHeavyDataSource = {
    // TODO what's the meaning of remove a node? sometimes causes node not found
    //table.remove(toRemove.map(_.bytes).toList)

    var txn: Txn[ByteBuffer] = null
    try {
      txn = env.txnWrite()
      toUpsert foreach {
        case (key, tval @ TVal(data, _, _)) =>
          if (isKeyTheValueHash) {
            val sKey = sliceBytes(key.bytes)
            keyWriteBuf.put(sKey).flip()
          } else {
            keyWriteBuf.put(key.bytes).flip()
          }

          if (isWithTimestamp) {
            ensureValueBufferSize(data.length + 8)
            dataWriteBuf.putLong(writingTimestamp).put(data).flip()
            cache.put(key, TVal(data, -1, writingTimestamp))
          } else {
            ensureValueBufferSize(data.length)
            dataWriteBuf.put(data).flip()
            cache.put(key, tval)
          }

          table.put(txn, keyWriteBuf, dataWriteBuf)
          keyWriteBuf.clear()
          dataWriteBuf.clear()
      }
      txn.commit()
    } catch {
      case ex: Throwable =>
        if (txn ne null) txn.abort()
        log.error(ex, ex.getMessage)
    } finally {
      if (txn ne null) txn.close()
    }

    this
  }

  def cacheHitRate = cache.hitRate
  def cacheReadCount = cache.readCount
  def resetCacheHitRate() = cache.resetHitRate()

  private def sliceBytes(bytes: Array[Byte]) = {
    val slice = Array.ofDim[Byte](4)
    System.arraycopy(bytes, 0, slice, 0, 4)
    slice
  }

  private def ensureValueBufferSize(size: Int): Unit = {
    if (dataWriteBuf.remaining < size) {
      dataWriteBuf = ByteBuffer.allocateDirect(size * 2)
    }
  }

  def close() {
    table.close()
  }
}

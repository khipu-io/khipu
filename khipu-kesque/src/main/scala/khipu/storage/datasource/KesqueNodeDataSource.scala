package khipu.storage.datasource

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.server.LogAppendResult
import kafka.utils.Logging
import khipu.TVal
import khipu.crypto
import kesque.Kesque
import kesque.KesqueIndexLmdb
import kesque.KesqueIndexRocksdb
import khipu.Hash
import khipu.util.Clock
import khipu.util.FIFOCache
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.DefaultRecord
import org.apache.kafka.common.record.DefaultRecordBatch
import org.apache.kafka.common.record.SimpleRecord
import org.lmdbjava.Env

final class KesqueNodeDataSource(
    val topic:       String,
    kesqueDb:        Kesque,
    lmdbOrRocksdb:   Either[Env[ByteBuffer], File],
    fetchMaxBytes:   Int                           = kesque.DEFAULT_FETCH_MAX_BYTES,
    compressionType: CompressionType               = CompressionType.NONE,
    cacheSize:       Int                           = 10000
) extends BlockDataSource[Hash, Array[Byte]] with Logging {
  type This = KesqueNodeDataSource

  private val cache = new FIFOCache[Hash, TVal](cacheSize)
  private val index = lmdbOrRocksdb match {
    case Left(lmdbEnv)      => new KesqueIndexLmdb(lmdbEnv, topic)
    case Right(rocksdbHome) => new KesqueIndexRocksdb(rocksdbHome, topic, useShortKey = true)
  }

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  val clock = new Clock()

  def get(key: Hash): Option[Array[Byte]] = {
    try {
      readLock.lock

      val keyBytes = key.bytes
      cache.get(key) match {
        case None =>
          val start = System.nanoTime

          var offsets = index.get(keyBytes)
          var foundValue: Option[TVal] = None
          while (foundValue.isEmpty && offsets.nonEmpty) {
            val offset = offsets.head
            val (topicPartition, result) = kesqueDb.read(topic, offset, fetchMaxBytes).head
            val recs = result.info.records.records.iterator
            // NOTE: the records usually do not start from the fecth-offset, 
            // the expected record may be near the tail of recs
            //println(s"\n======= $offset -> $result")
            while (recs.hasNext) {
              val rec = recs.next
              //print(s"${rec.offset},")

              if (rec.offset == offset) {
                if (rec.hasValue) {
                  val data = kesque.getBytes(rec.value)
                  val fullKey = crypto.kec256(data)
                  if (java.util.Arrays.equals(fullKey, keyBytes)) {
                    foundValue = Some(TVal(data, offset, 0))
                  }
                } else {
                  None
                }
              }
            }
            offsets = offsets.tail
          }

          foundValue foreach { tval => cache.put(key, tval) }

          clock.elapse(System.nanoTime - start)

          foundValue.map(_.value)
        case Some(tval) => Some(tval.value)
      }

    } finally {
      readLock.unlock()
    }
  }

  def update(toRemove: Iterable[Hash], toUpsert: Iterable[(Hash, Array[Byte])]): KesqueNodeDataSource = {

    // we'll keep the batch size not exceeding fetchMaxBytes to get better random read performance
    var batchedRecords = Vector[(Seq[(Hash, Array[Byte])], Seq[SimpleRecord])]()

    var size = DefaultRecordBatch.RECORD_BATCH_OVERHEAD
    var offsetDelta = 0
    var firstTimestamp = Long.MinValue

    // prepare simple records, filter no changed ones
    var tkvs = Vector[(Hash, Array[Byte])]()
    var records = Vector[SimpleRecord]()
    var keyToPrevOffset = Map[Hash, Long]()
    toUpsert foreach {
      case kv @ (key, value) =>
        val keyBytes = key.bytes
        cache.get(key) match {
          case Some(TVal(prevValue, prevOffset, _)) =>
            if (isValueChanged(value, prevValue)) {
              val record = new SimpleRecord(null, value)
              if (firstTimestamp == Long.MinValue) {
                firstTimestamp = 0
              }
              val (newSize, estimatedSize) = estimateSizeInBytes(size, firstTimestamp, offsetDelta, record)
              if (estimatedSize < fetchMaxBytes) {
                tkvs :+= kv
                records :+= record
                keyToPrevOffset += (key -> prevOffset)

                size = newSize
                offsetDelta += 1
              } else {
                batchedRecords :+= (tkvs, records)
                tkvs = Vector[(Hash, Array[Byte])]()
                records = Vector[SimpleRecord]()

                tkvs :+= kv
                records :+= record
                keyToPrevOffset += (key -> prevOffset)

                size = DefaultRecordBatch.RECORD_BATCH_OVERHEAD
                offsetDelta = 0
                val (firstSize, _) = estimateSizeInBytes(size, firstTimestamp, offsetDelta, record)
                size = firstSize
                offsetDelta += 1
              }

              // TODO should only happen when value is set to empty, i.e. removed?
              // remove records of prevOffset from memory?
            } else {
              debug(s"$topic: value not changed. cache: hit ${cache.hitRate}, miss ${cache.missRate}}")
            }
          case None =>
            val record = new SimpleRecord(null, value)
            if (firstTimestamp == Long.MinValue) {
              firstTimestamp = 0
            }
            val (newSize, estimatedSize) = estimateSizeInBytes(size, firstTimestamp, offsetDelta, record)
            //println(s"$newSize, $estimatedSize")
            if (estimatedSize < fetchMaxBytes) {
              tkvs :+= kv
              records :+= record

              size = newSize
              offsetDelta += 1
            } else {
              batchedRecords :+= (tkvs, records)
              tkvs = Vector[(Hash, Array[Byte])]()
              records = Vector[SimpleRecord]()

              tkvs :+= kv
              records :+= record

              size = DefaultRecordBatch.RECORD_BATCH_OVERHEAD
              offsetDelta = 0
              val (firstSize, _) = estimateSizeInBytes(size, firstTimestamp, offsetDelta, record)
              size = firstSize
              offsetDelta += 1
            }
        }
    }

    if (records.nonEmpty) {
      batchedRecords :+= (tkvs, records)
    }

    // write to log file
    batchedRecords map {
      case (tkvs, recs) =>
        if (recs.nonEmpty) {
          writeRecords(tkvs, recs, keyToPrevOffset)
        } else {
          0
        }
    } sum

    this
  }

  /**
   * @see org.apache.kafka.common.record.AbstractRecords#estimateSizeInBytes
   */
  private def estimateSizeInBytes(prevSize: Int, firstTimestamp: Long, offsetDelta: Int, record: SimpleRecord): (Int, Int) = {
    val timestampDelta = record.timestamp - firstTimestamp
    val size = prevSize + DefaultRecord.sizeInBytes(offsetDelta, timestampDelta, record.key, record.value, record.headers)

    val estimateSize = if (compressionType == CompressionType.NONE) {
      size
    } else {
      math.min(math.max(size / 2, 1024), 1 << 16)
    }
    (size, estimateSize)
  }

  private def writeRecords(kvs: Seq[(Hash, Array[Byte])], records: Seq[SimpleRecord], keyToPrevOffset: Map[Hash, Long]): Int = {
    try {
      writeLock.lock()

      // write simple records and create index records
      val indexRecords = kesqueDb.write(topic, records, compressionType).foldLeft(Vector[Vector[(Array[Byte], Long)]]()) {
        case (indexRecords, (topicPartition, LogAppendResult(appendInfo, Some(ex)))) =>
          error(ex.getMessage, ex) // TODO
          indexRecords

        case (indexRecords, (topicPartition, LogAppendResult(appendInfo, None))) =>
          if (appendInfo.numMessages > 0) {
            val firstOffert = appendInfo.firstOffset.get
            val (lastOffset, idxRecords) = kvs.foldLeft(firstOffert, Vector[(Array[Byte], Long)]()) {
              case ((longOffset, idxRecords), (key, value)) =>
                val offset = longOffset.toInt
                val keyBytes = key.bytes
                val indexRecord = (keyBytes -> longOffset)

                // check if there is prevOffset, will remove it (replace it with current one)
                keyToPrevOffset.get(key) match {
                  case Some(prevOffset) => index.remove(keyBytes, prevOffset)
                  case None             =>
                }
                cache.put(key, TVal(value, offset, 0L))
                (offset + 1, idxRecords :+ indexRecord)
            }

            assert(appendInfo.lastOffset == lastOffset - 1, s"lastOffset(${appendInfo.lastOffset}) != ${lastOffset - 1}, firstOffset is ${appendInfo.firstOffset}, numOfMessages is ${appendInfo.numMessages}, numRecords is ${records.size}, appendInfo: $appendInfo")

            indexRecords :+ idxRecords
          } else {
            indexRecords
          }
      }

      // write index records
      index.put(indexRecords.flatten)

      indexRecords.map(_.size).sum
    } finally {
      writeLock.unlock()
    }
  }

  private def isValueChanged(v1: Array[Byte], v2: Array[Byte]) = {
    if ((v1 eq null) && (v2 eq null)) {
      false
    } else if ((v1 eq null) || (v2 eq null)) {
      true
    } else {
      !java.util.Arrays.equals(v1, v2)
    }
  }

  def count = index.count

  def cacheHitRate = cache.hitRate
  def cacheReadCount = cache.readCount
  def resetCacheHitRate() = cache.resetHitRate()

  def close() {
    index.close()
  }
}


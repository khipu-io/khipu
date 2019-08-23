package kesque

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.server.LogAppendResult
import kafka.utils.Logging
import khipu.Hash
import khipu.TKeyVal
import khipu.TVal
import khipu.util.FIFOCache
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.DefaultRecord
import org.apache.kafka.common.record.DefaultRecordBatch
import org.apache.kafka.common.record.SimpleRecord
import org.lmdbjava.Env
import org.rocksdb.OptimisticTransactionDB

object KesqueTable {
  val fetchMaxBytesInLoadOffsets = 100 * 1024 * 1024 // 100M
  val defaultFetchMaxBytes = 4 * 1024 // 4K - the size of SSD block
}
final class KesqueTable private[kesque] (
    topics:          Array[String],
    kesqueDb:        Kesque,
    lmdbOrRocksdb:   Either[Env[ByteBuffer], OptimisticTransactionDB],
    withTimeToKey:   Boolean,
    fetchMaxBytes:   Int                                              = KesqueTable.defaultFetchMaxBytes,
    compressionType: CompressionType                                  = CompressionType.NONE,
    cacheSize:       Int                                              = 10000
) extends Logging {
  private val (caches, indexes, topicToCol) = init()

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  private def init() = {
    val caches = Array.ofDim[FIFOCache[Hash, TVal]](topics.length)
    val indexes = Array.ofDim[KesqueIndex](topics.length)
    var topicToCol = Map[String, Int]()
    var i = 0
    while (i < topics.length) {
      caches(i) = new FIFOCache[Hash, TVal](cacheSize)
      indexes(i) = lmdbOrRocksdb match {
        case Left(lmdbEnv)       => new KesqueIndexLmdb(lmdbEnv, topics(i))
        case Right(rocksdbTable) => new KesqueIndexRocksdb(rocksdbTable, topics(i))
      }
      topicToCol += (topics(i) -> i)
      i += 1
    }
    (caches, indexes, topicToCol)
  }

  private def indexTopic(topic: String) = topic + "_idx"

  def read(keyBytes: Array[Byte], topic: String, bypassCache: Boolean = false): Option[TVal] = {
    try {
      readLock.lock

      val col = topicToCol(topic)
      val key = Hash(keyBytes)
      caches(col).get(key) match {
        case None =>
          var offsets = indexes(col).get(keyBytes)
          var foundValue: Option[TVal] = None
          while (foundValue.isEmpty && offsets.nonEmpty) {
            val offset = offsets.head
            val kafkaTopic = topics(col) // kafka topic directory name
            val (topicPartition, result) = kesqueDb.read(kafkaTopic, offset, fetchMaxBytes).head
            val recs = result.info.records.records.iterator
            // NOTE: the records usually do not start from the fecth-offset, 
            // the expected record may be near the tail of recs
            //println(s"\n======= $offset -> $result")
            while (recs.hasNext) {
              val rec = recs.next
              //print(s"${rec.offset},")
              if (rec.offset == offset && java.util.Arrays.equals(kesque.getBytes(rec.key), keyBytes)) {
                foundValue = if (rec.hasValue) {
                  Some(TVal(kesque.getBytes(rec.value), offset.toInt, rec.timestamp))
                } else {
                  None
                }
              }
            }
            offsets = offsets.tail
          }

          if (!bypassCache) {
            foundValue foreach { tv => caches(col).put(key, tv) }
          }

          foundValue
        case Some(value) => Some(value)
      }

    } finally {
      readLock.unlock()
    }
  }

  def write(kvs: Iterable[TKeyVal], topic: String, fileno: Int = 0): Int = {
    val col = topicToCol(topic)

    // we'll keep the batch size not exceeding fetchMaxBytes to get better random read performance
    var bacthedRecords = Vector[(Seq[TKeyVal], Seq[SimpleRecord])]()

    var size = DefaultRecordBatch.RECORD_BATCH_OVERHEAD
    var offsetDelta = 0
    var firstTimestamp = Long.MinValue

    // prepare simple records, filter no changed ones
    var tkvs = Vector[TKeyVal]()
    var records = Vector[SimpleRecord]()
    var keyToPrevOffset = Map[Hash, Int]()
    var lastRecordsAdded = false
    kvs foreach {
      case kv @ TKeyVal(keyBytes, value, offset, timestamp) =>
        val key = Hash(keyBytes)
        caches(col).get(key) match {
          case Some(TVal(prevValue, prevOffset, _)) =>
            if (isValueChanged(value, prevValue)) {
              val record = new SimpleRecord(timestamp, keyBytes, value)
              if (firstTimestamp == Long.MinValue) {
                firstTimestamp = timestamp
              }
              val (newSize, estimatedSize) = estimateSizeInBytes(size, firstTimestamp, offsetDelta, record)
              if (estimatedSize < fetchMaxBytes) {
                tkvs :+= kv
                records :+= record
                keyToPrevOffset += (key -> prevOffset)

                size = newSize
                offsetDelta += 1
              } else {
                bacthedRecords :+= (tkvs, records)
                tkvs = Vector[TKeyVal]()
                records = Vector[SimpleRecord]()
                lastRecordsAdded = false

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
              debug(s"$topic: value not changed. cache: hit ${caches(col).hitRate}, miss ${caches(col).missRate}}")
            }
          case None =>
            val record = new SimpleRecord(timestamp, keyBytes, value)
            if (firstTimestamp == Long.MinValue) {
              firstTimestamp = timestamp
            }
            val (newSize, estimatedSize) = estimateSizeInBytes(size, firstTimestamp, offsetDelta, record)
            //println(s"$newSize, $estimatedSize")
            if (estimatedSize < fetchMaxBytes) {
              tkvs :+= kv
              records :+= record

              size = newSize
              offsetDelta += 1
            } else {
              bacthedRecords :+= (tkvs, records)
              tkvs = Vector[TKeyVal]()
              records = Vector[SimpleRecord]()
              lastRecordsAdded = false

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

    if (!lastRecordsAdded) {
      bacthedRecords :+= (tkvs, records)
    }

    // write to log file
    bacthedRecords map {
      case (tkvs, recs) =>
        if (recs.nonEmpty) {
          writeRecords(tkvs, recs, keyToPrevOffset, col, fileno)
        } else {
          0
        }
    } sum
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

  private def writeRecords(kvs: Seq[TKeyVal], records: Seq[SimpleRecord], keyToPrevOffset: Map[Hash, Int], col: Int, fileno: Int): Int = {
    try {
      writeLock.lock()

      val kafkaTopic = topics(col)
      // write simple records and create index records
      val indexRecords = kesqueDb.write(kafkaTopic, records, compressionType).foldLeft(Vector[Vector[(Array[Byte], Long)]]()) {
        case (indexRecords, (topicPartition, LogAppendResult(appendInfo, Some(ex)))) =>
          error(ex.getMessage, ex) // TODO
          indexRecords

        case (indexRecords, (topicPartition, LogAppendResult(appendInfo, None))) =>
          if (appendInfo.numMessages > 0) {
            val firstOffert = appendInfo.firstOffset.get
            val (lastOffset, idxRecords) = kvs.foldLeft(firstOffert, Vector[(Array[Byte], Long)]()) {
              case ((longOffset, idxRecords), TKeyVal(keyBytes, value, _, timestamp)) =>
                val offset = longOffset.toInt
                val key = Hash(keyBytes)
                val indexRecord = (keyBytes -> longOffset)

                // check if there is prevOffset, will remove it (replace it with current one)
                keyToPrevOffset.get(key) match {
                  case Some(prevOffset) => indexes(col).remove(keyBytes, prevOffset)
                  case None             =>
                }
                caches(col).put(key, TVal(value, offset, timestamp))
                (offset + 1, idxRecords :+ indexRecord)
            }

            assert(appendInfo.lastOffset == lastOffset - 1, s"lastOffset(${appendInfo.lastOffset}) != ${lastOffset - 1}, firstOffset is ${appendInfo.firstOffset}, numOfMessages is ${appendInfo.numMessages}, numRecords is ${records.size}, appendInfo: $appendInfo")

            indexRecords :+ idxRecords
          } else {
            indexRecords
          }
      }

      // write index records
      indexes(col).put(indexRecords.flatten)

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
}

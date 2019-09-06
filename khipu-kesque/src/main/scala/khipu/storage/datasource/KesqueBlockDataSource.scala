package khipu.storage.datasource

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.server.LogAppendResult
import kafka.utils.Logging
import kesque.Kesque
import khipu.util.Clock
import khipu.util.FIFOCache
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.DefaultRecord
import org.apache.kafka.common.record.DefaultRecordBatch
import org.apache.kafka.common.record.SimpleRecord

final class KesqueBlockDataSource(
    val topic:       String,
    kesqueDb:        Kesque,
    cacheSize:       Int,
    fetchMaxBytes:   Int             = kesque.DEFAULT_FETCH_MAX_BYTES,
    compressionType: CompressionType = CompressionType.NONE
) extends BlockDataSource with Logging {
  type This = KesqueBlockDataSource

  private val cache = new FIFOCache[Long, Array[Byte]](cacheSize)

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  val clock = new Clock()

  info(s"Table $topic best block number $bestBlockNumber")

  def get(key: Long): Option[Array[Byte]] = {
    try {
      readLock.lock

      val keyBytes = ByteBuffer.allocate(8).putLong(key).array
      cache.get(key) match {
        case None =>
          val start = System.nanoTime

          var foundValue: Option[Array[Byte]] = None
          val (topicPartition, result) = kesqueDb.read(topic, key, fetchMaxBytes).head
          val recs = result.info.records.records.iterator
          // NOTE: the records usually do not start from the fecth-offset, 
          // the expected record may be near the tail of recs
          //println(s"\n======= $offset -> $result")
          while (foundValue.isEmpty && recs.hasNext) {
            val rec = recs.next
            //print(s"${rec.offset},")
            if (rec.offset == key) {
              if (rec.hasValue) {
                val data = kesque.getBytes(rec.value)
                val theKey = kesque.getBytes(rec.key)
                if (!java.util.Arrays.equals(theKey, keyBytes)) {
                  warn(s"key ${ByteBuffer.wrap(theKey).getLong()} does not equals offset $key during get")
                }
                foundValue = Some(data)
              }
            }
          }

          foundValue foreach { v => cache.put(key, v) }

          clock.elapse(System.nanoTime - start)

          foundValue
        case some => some
      }

    } finally {
      readLock.unlock()
    }
  }

  def update(toRemove: Iterable[Long], toUpsert: Iterable[(Long, Array[Byte])]): KesqueBlockDataSource = {

    // we'll keep the batch size not exceeding fetchMaxBytes to get better random read performance
    var batchedRecords = Vector[(Seq[(Long, Array[Byte])], Seq[SimpleRecord])]()

    var size = DefaultRecordBatch.RECORD_BATCH_OVERHEAD
    var offsetDelta = 0
    var firstTimestamp = Long.MinValue

    // prepare simple records, filter no changed ones
    var kvs = Vector[(Long, Array[Byte])]()
    var records = Vector[SimpleRecord]()
    toUpsert foreach {
      case kv @ (key, value) =>
        val keyBytes = ByteBuffer.allocate(8).putLong(key).array
        val record = new SimpleRecord(keyBytes, value)
        if (firstTimestamp == Long.MinValue) {
          firstTimestamp = 0
        }
        val (newSize, estimatedSize) = estimateSizeInBytes(size, firstTimestamp, offsetDelta, record)
        //println(s"$newSize, $estimatedSize")
        if (estimatedSize < fetchMaxBytes) {
          kvs :+= kv
          records :+= record

          size = newSize
          offsetDelta += 1
        } else {
          batchedRecords :+= (kvs, records)
          kvs = Vector[(Long, Array[Byte])]()
          records = Vector[SimpleRecord]()

          kvs :+= kv
          records :+= record

          size = DefaultRecordBatch.RECORD_BATCH_OVERHEAD
          offsetDelta = 0
          val (firstSize, _) = estimateSizeInBytes(size, firstTimestamp, offsetDelta, record)
          size = firstSize
          offsetDelta += 1
        }
    }

    if (records.nonEmpty) {
      batchedRecords :+= (kvs, records)
    }

    // write to log file
    batchedRecords map {
      case (tkvs, recs) =>
        if (recs.nonEmpty) {
          writeRecords(tkvs, recs)
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

  private def writeRecords(kvs: Seq[(Long, Array[Byte])], records: Seq[SimpleRecord]): Int = {
    try {
      writeLock.lock()

      val count = kesqueDb.write(topic, records, compressionType).foldLeft(0) {
        case (count, (topicPartition, LogAppendResult(appendInfo, Some(ex)))) =>
          error(ex.getMessage, ex) // TODO
          count

        case (count, (topicPartition, LogAppendResult(appendInfo, None))) =>
          if (appendInfo.numMessages > 0) {
            val firstOffert = appendInfo.firstOffset.get
            val lastOffset = kvs.foldLeft(firstOffert) {
              case ((offset), (key, value)) =>
                if (offset != key) {
                  warn(s"key $key does not equals offset $offset during put")
                }
                val keyBytes = ByteBuffer.allocate(8).putLong(key).array

                cache.put(key, value)
                offset + 1
            }

            assert(appendInfo.lastOffset == lastOffset - 1, s"lastOffset(${appendInfo.lastOffset}) != ${lastOffset - 1}, firstOffset is ${appendInfo.firstOffset}, numOfMessages is ${appendInfo.numMessages}, numRecords is ${records.size}, appendInfo: $appendInfo")

            count + 1
          } else {
            count
          }
      }

      // write index records
      count
    } finally {
      writeLock.unlock()
    }
  }

  def count = kesqueDb.getLogEndOffset(topic)
  def bestBlockNumber = count - 1 // block number starts from 0

  def cacheHitRate = cache.hitRate
  def cacheReadCount = cache.readCount
  def resetCacheHitRate() = cache.resetHitRate()

  def stop() {}

}


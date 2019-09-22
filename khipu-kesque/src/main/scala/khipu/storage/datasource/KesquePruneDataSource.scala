package khipu.storage.datasource

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.server.LogAppendResult
import kafka.utils.Logging
import khipu.TKeyVal
import kesque.Kesque
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.SimpleRecord
import scala.collection.mutable

object KesquePruneDataSource {
  private val FETCH_MAX_BYTES_IN_BACTH = 50 * 1024 * 1024 // 52428800, 50M
}
class KesquePruneDataSource(_topic: String, kesqueDb: Kesque) extends Logging {
  import KesquePruneDataSource._

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  val topic = _topic + "_prune"

  def load(tillBlock: Long): (mutable.HashSet[Long], Long) = {
    println(s"[comp] $topic load from offset 0 till block #$tillBlock ...")
    val start = System.nanoTime

    val ret = new mutable.HashSet[Long]()
    var tillOffset = 0L

    var blockNumber = 0L
    var offset = 0L
    var nRead = 0
    do {
      val (lastOffset, recs) = readBatch(topic, offset, FETCH_MAX_BYTES_IN_BACTH)

      var i = 0
      while (i < recs.length && blockNumber <= tillBlock) {
        val TKeyVal(keyBytes, value, thisOffset) = recs(i)
        val key = ByteBuffer.wrap(keyBytes).getLong()
        blockNumber = ByteBuffer.wrap(value).getLong()
        tillOffset = thisOffset

        i += 1
      }

      nRead = recs.length
      offset = lastOffset + 1
    } while (nRead > 0 && blockNumber <= tillBlock)

    val elapsed = (System.nanoTime - start) / 1000000000
    println(s"[comp] $topic load done in ${elapsed}s")

    (ret, tillOffset)
  }

  def append(toAppend: Iterable[(Long, Long)]) = {
    // prepare simple records, filter no changed ones
    var records = Vector[SimpleRecord]()
    toAppend foreach {
      case kv @ (key, blockNumber) =>
        val keyBytes = ByteBuffer.allocate(8).putLong(key).array
        val valBytes = ByteBuffer.allocate(8).putLong(blockNumber).array
        val record = new SimpleRecord(keyBytes, valBytes)
        records :+= record
    }

    // write to log file
    if (records.nonEmpty) {
      writeRecords(records)
    } else {
      0
    }
  }

  private def writeRecords(records: Seq[SimpleRecord]): Int = {
    try {
      writeLock.lock()

      val count = kesqueDb.write(topic, records, CompressionType.NONE).foldLeft(0) {
        case (count, (topicPartition, LogAppendResult(appendInfo, Some(ex)))) =>
          error(ex.getMessage, ex) // TODO
          count

        case (count, (topicPartition, LogAppendResult(appendInfo, None))) =>
          if (appendInfo.numMessages > 0) {
            val firstOffert = appendInfo.firstOffset.get
            val lastOffset = firstOffert + records.size

            assert(appendInfo.lastOffset == lastOffset - 1, s"lastOffset(${appendInfo.lastOffset}) != ${lastOffset - 1}, firstOffset is ${appendInfo.firstOffset}, numOfMessages is ${appendInfo.numMessages}, numRecords is ${records.size}, appendInfo: $appendInfo")

            count + 1
          } else {
            count
          }
      }

      count
    } finally {
      writeLock.unlock()
    }
  }

  private def readBatch(topic: String, fromOffset: Long, fetchMaxBytes: Int): (Long, Array[TKeyVal]) = {
    try {
      readLock.lock()

      kesqueDb.readBatch(topic, fromOffset, fetchMaxBytes)
    } finally {
      readLock.unlock()
    }
  }

  def count = kesqueDb.getLogEndOffset(topic)
}

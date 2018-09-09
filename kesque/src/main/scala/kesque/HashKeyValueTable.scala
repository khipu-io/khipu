package kesque

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.server.LogAppendResult
import kafka.utils.Logging
import org.apache.kafka.common.record.SimpleRecord
import scala.collection.mutable

object HashKeyValueTable {
  val fetchMaxBytesInLoadOffsets = 100 * 1024 * 1024 // 100M

  def intToBytes(v: Int) = ByteBuffer.allocate(4).putInt(v).array
  def bytesToInt(v: Array[Byte]) = ByteBuffer.wrap(v).getInt
}
final class HashKeyValueTable private[kesque] (topics: Array[String], db: Kesque, withTimeToKey: Boolean, fetchMaxBytes: Int = 102400, cacheSize: Int = 10000) extends Logging {
  import HashKeyValueTable._

  private val hashOffsets = new HashOffsets(200, topics.length)
  private var timeIndex = Array.ofDim[Array[Byte]](200) // time to key table, should use first topic to initially create it
  private val caches = Array.ofDim[FIFOCache[Hash, (TVal, Int)]](topics.length)
  private val (topicIndex, _) = topics.foldLeft(Map[String, Int](), 0) {
    case ((map, i), topic) => (map + (topic -> i), i + 1)
  }
  private val indexTopics = topics map indexTopic

  private val lock = new ReentrantReadWriteLock()
  private val readLock = lock.readLock
  private val writeLock = lock.writeLock

  private def indexTopic(topic: String) = topic + "_idx"

  private class LoadIndexesTask(valueIndex: Int, topic: String) extends Thread {
    override def run() {
      loadOffsets(valueIndex)
    }
  }

  loadIndexes()

  private def loadIndexes() {
    var tasks = List[Thread]()
    var n = 0
    while (n < topics.length) {
      val topic = topics(n)
      caches(n) = new FIFOCache[Hash, (TVal, Int)](cacheSize)

      tasks = new LoadIndexesTask(n, topic) :: tasks

      n += 1
    }

    val timeIndexTask = if (withTimeToKey) {
      List(new Thread() {
        override def run() {
          loadTimeIndex
        }
      })
    } else {
      Nil
    }

    timeIndexTask ::: tasks foreach { _.start() }
    timeIndexTask ::: tasks foreach { _.join() }
  }

  private def loadOffsets(valueIndex: Int) {
    val indexTopic = indexTopics(valueIndex)

    info(s"Loading index of ${topics(valueIndex)}")
    val start = System.currentTimeMillis

    var count = 0
    db.iterateOver(indexTopic, 0, fetchMaxBytesInLoadOffsets) {
      case (offset, Record(hashCode, recordOffset, timestamp)) =>
        if (hashCode != null && recordOffset != null) {
          hashOffsets.put(bytesToInt(hashCode), bytesToInt(recordOffset), valueIndex)
          count += 1
        }
    }

    info(s"Loaded index of ${topics(valueIndex)} in ${System.currentTimeMillis - start} ms, count $count, size ${hashOffsets.size}")
  }

  private def loadTimeIndex() {
    val topic = topics(0)

    info(s"Loading time index from $topic")
    val start = System.currentTimeMillis

    var count = 0
    db.iterateOver(topic, 0, fetchMaxBytesInLoadOffsets) {
      case (offset, Record(key, value, timestamp)) =>
        if (key != null && value != null) {
          putTimeToKey(timestamp, key)
          count += 1
        }
    }

    info(s"Loaded time index from $topic in ${System.currentTimeMillis - start} ms, count $count")
  }

  def putTimeToKey(timestamp: Long, key: Array[Byte]) {
    try {
      writeLock.lock()

      if (timestamp > timeIndex.length - 1) {
        val newArr = Array.ofDim[Array[Byte]]((timeIndex.length * 1.2).toInt)
        System.arraycopy(timeIndex, 0, newArr, 0, timeIndex.length)
        timeIndex = newArr
      }
      timeIndex(timestamp.toInt) = key
    } finally {
      writeLock.unlock()
    }
  }

  def getKeyByTime(timestamp: Long): Option[Array[Byte]] = {
    try {
      readLock.lock()

      if (!withTimeToKey) {
        None
      } else {
        if (timestamp >= 0 && timestamp < timeIndex.length) {
          Option(timeIndex(timestamp.toInt))
        } else {
          None
        }
      }
    } finally {
      readLock.unlock()
    }
  }

  def read(key: Array[Byte], topic: String): Option[TVal] = {
    try {
      readLock.lock

      val valueIndex = topicIndex(topic)
      caches(valueIndex).get(Hash(key)) match {
        case None =>
          val hash = Hash(key)
          hashOffsets.get(hash.hashCode, valueIndex) match {
            case IntIntsMap.NO_VALUE => None
            case offsets =>
              var foundValue: Option[TVal] = None
              var foundOffset = Int.MinValue
              var i = offsets.length - 1 // loop backward to find newest one  
              while (i >= 0 && foundValue.isEmpty) {
                val offset = offsets(i)
                val (topicPartition, result) = db.read(topic, offset, fetchMaxBytes).head
                val recs = result.info.records.records.iterator
                while (recs.hasNext) { // NOTE: the records are offset resversed !!
                  val rec = recs.next
                  if (rec.offset == offset && java.util.Arrays.equals(db.getBytes(rec.key), key)) {
                    foundOffset = offset
                    foundValue = if (rec.hasValue) Some(TVal(db.getBytes(rec.value), rec.timestamp)) else None
                  }
                }
                i -= 1
              }

              foundValue foreach { x =>
                caches(valueIndex).put(hash, (x, foundOffset))
              }

              foundValue
          }
        case Some((value, offset)) => Some(value)
      }
    } finally {
      readLock.unlock()
    }
  }

  def write(kvs: Iterable[Record], topic: String): Iterable[Long] = {
    try {
      writeLock.lock()

      val valueIndex = topicIndex(topic)
      val (records, keyToPrevOffsets) = kvs.foldLeft(Vector[SimpleRecord](), mutable.Map[Hash, Int]()) {
        case (acc @ (records, keyToPrevOffsets), Record(k, v, t)) =>
          val hash = Hash(k)
          caches(valueIndex).get(hash) match {
            case Some((tval, prevOffset)) =>
              if (isValueChanged(v, tval.value)) {
                val record = if (t < 0) new SimpleRecord(k, v) else new SimpleRecord(t, k, v)
                (records :+ record, keyToPrevOffsets += hash -> prevOffset)
              } else {
                //log.debug(s"$topic: value not changed. cache hit ${caches(valueIndex).hitCount}, miss ${caches(valueIndex).missCount}, size ${caches(valueIndex).size}")
                acc
              }
            case None =>
              val record = if (t < 0) new SimpleRecord(k, v) else new SimpleRecord(t, k, v)
              (records :+ record, keyToPrevOffsets)
          }
      }

      val indexRecords = db.write(topic, records).foldLeft(Vector[Vector[SimpleRecord]]()) {
        case (indexRecords, (topicPartition, LogAppendResult(info, Some(ex)))) =>
          error(ex.getMessage, ex) // TODO
          indexRecords

        case (indexRecords, (topicPartition, LogAppendResult(info, None))) =>
          val firstOffert = info.firstOffset.getOrElse(0L)
          val (lastOffset, parIdxRecords) = kvs.foldLeft(firstOffert, Vector[SimpleRecord]()) {
            case ((offset, parIdxRecords), Record(k, v, t)) =>
              val hash = Hash(k)
              val hashCode = hash.hashCode
              val indexRecord = new SimpleRecord(intToBytes(hashCode), intToBytes(offset.toInt))

              keyToPrevOffsets.get(hash) match {
                case Some(prevOffset) => // there is prevOffset, will also remove it (replace it with current one)
                  hashOffsets.replace(hashCode, prevOffset.toInt, offset.toInt, valueIndex)
                case None => // there is none prevOffset
                  hashOffsets.put(hashCode, offset.toInt, valueIndex)
              }
              caches(valueIndex).put(hash, (TVal(v, t), offset.toInt))
              (offset + 1, parIdxRecords :+ indexRecord)
          }
          if (kvs.nonEmpty) {
            assert(info.lastOffset == lastOffset - 1, s"lastOffset(${info.lastOffset}) != ${lastOffset - 1}, firstOffset is ${info.firstOffset}")
            val count = info.lastOffset - firstOffert + 1
          }

          indexRecords :+ parIdxRecords
      }

      db.write(indexTopics(valueIndex), indexRecords.flatten) map {
        case (topicPartition, LogAppendResult(appendInfo, Some(ex))) =>
          error(ex.getMessage, ex) // TODO
        case (topicPartition, LogAppendResult(appendInfo, None)) =>
          debug(s"$topic: append index records $indexRecords")
      }

      indexRecords.map(_.size.toLong)
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

  def remove(keys: Seq[Array[Byte]], topic: String) = {
    try {
      writeLock.lock()

      val valueIndex = topicIndex(topic)
      caches(valueIndex).remove(keys.map(Hash(_)))
      val records = keys map { key => new SimpleRecord(key, null) }
      val indexRecords = db.write(topic, records).foldLeft(Vector[Vector[SimpleRecord]]()) {
        case (indexRecords, (topicPartition, LogAppendResult(info, Some(ex)))) =>
          error(ex.getMessage, ex) // TODO
          indexRecords

        case (indexRecords, (topicPartition, LogAppendResult(info, None))) =>
          val firstOffert = info.firstOffset.getOrElse(0L)
          val (lastOffset, parIdxRecords) = keys.foldLeft(firstOffert, Vector[SimpleRecord]()) {
            case ((offset, parIdxRecords), k) =>
              val hash = Hash(k)
              val hashCode = hash.hashCode
              val indexRecord = new SimpleRecord(intToBytes(hashCode), intToBytes(offset.toInt))

              hashOffsets.put(hashCode, offset.toInt, valueIndex)
              // should also find and remove previous offsets? it will cost 
              // a i/o reading. Not necessary or do it in write
              (offset + 1, parIdxRecords :+ indexRecord)
          }
          if (keys.nonEmpty) {
            assert(info.lastOffset == lastOffset - 1, s"lastOffset ${info.lastOffset} != ${lastOffset - 1} ")
            val count = info.lastOffset - firstOffert + 1
          }

          indexRecords :+ parIdxRecords
      }

      db.write(indexTopics(valueIndex), indexRecords.flatten) map {
        case (topicPartition, LogAppendResult(appendInfo, Some(ex))) =>
          error(ex.getMessage, ex) // TODO
        case (topicPartition, LogAppendResult(appendInfo, None)) =>
          debug(s"$topic: append index records $indexRecords")
      }

      indexRecords.map(_.size.toLong)
    } finally {
      writeLock.unlock()
    }
  }

  def iterateOver(fetchOffset: Long, topic: String)(op: (Long, Record) => Unit) = {
    try {
      readLock.lock()

      db.iterateOver(topic, fetchOffset, fetchMaxBytes)(op)
    } finally {
      readLock.unlock()
    }
  }

  def readOnce(fetchOffset: Long, topic: String)(op: (Long, Record) => Unit) = {
    try {
      readLock.lock()

      db.readOnce(topic, fetchOffset, fetchMaxBytes)(op)
    } finally {
      readLock.unlock()
    }
  }

}

package kesque

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.server.LogAppendResult
import kafka.utils.Logging
import org.apache.kafka.common.record.DefaultRecordBatch
import org.apache.kafka.common.record.SimpleRecord
import scala.collection.mutable

object HashKeyValueTable {
  val fetchMaxBytesInLoadOffsets = 100 * 1024 * 1024 // 100M
  val defaultFetchMaxBytes = 256 * 1024 // 256K 

  def intToBytes(v: Int) = ByteBuffer.allocate(4).putInt(v).array
  def bytesToInt(v: Array[Byte]) = ByteBuffer.wrap(v).getInt
}
final class HashKeyValueTable private[kesque] (
    topics: Array[String], db: Kesque, withTimeToKey: Boolean,
    fetchMaxBytes: Int = HashKeyValueTable.defaultFetchMaxBytes, cacheSize: Int = 10000
) extends Logging {
  import HashKeyValueTable._

  private val hashOffsets = new HashOffsets(200, topics.length)

  /* time to key table, should be the first topic to initially create it */
  private var timeIndex = Array.ofDim[Array[Byte]](200)

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
      case (offset, TKeyVal(hashCode, recordOffset, timestamp)) =>
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
      case (offset, TKeyVal(key, value, timestamp)) =>
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
                while (recs.hasNext) { // NOTE: the records are offset reversed !!
                  val rec = recs.next
                  if (rec.offset == offset && java.util.Arrays.equals(db.getBytes(rec.key), key)) {
                    foundOffset = offset
                    foundValue = if (rec.hasValue) Some(TVal(db.getBytes(rec.value), rec.timestamp)) else None
                  }
                }
                i -= 1
              }

              foundValue foreach { tv =>
                caches(valueIndex).put(hash, (tv, foundOffset))
              }

              foundValue
          }
        case Some((value, offset)) => Some(value)
      }
    } finally {
      readLock.unlock()
    }
  }

  def write(kvs: Iterable[TKeyVal], topic: String) = {
    val topicIdx = topicIndex(topic)

    // used to calc recordBatch size
    val simpleRecords = new java.util.LinkedList[SimpleRecord]()

    // create simple records, filter no changed ones
    var recordBatches = Vector[(Vector[(TKeyVal, SimpleRecord)], Map[Hash, Int])]()
    var records = List[(TKeyVal, SimpleRecord)]()
    var keyToPrevOffsets = Map[Hash, Int]()
    val itr = kvs.iterator
    while (itr.hasNext) {
      val tkv @ TKeyVal(key, value, timestamp) = itr.next()
      val hash = Hash(key)
      caches(topicIdx).get(hash) match {
        case Some((TVal(prevValue, _), prevOffset)) =>
          if (isValueChanged(value, prevValue)) {
            val rec = if (timestamp < 0) new SimpleRecord(key, value) else new SimpleRecord(timestamp, key, value)
            simpleRecords.add(rec)
            records ::= tkv -> rec
            keyToPrevOffsets += hash -> prevOffset
          } else {
            debug(s"$topic: value not changed. cache: hit ${caches(topicIdx).hitCount}, miss ${caches(topicIdx).missCount}, size ${caches(topicIdx).size}")
          }
        case None =>
          val rec = if (timestamp < 0) new SimpleRecord(key, value) else new SimpleRecord(timestamp, key, value)
          simpleRecords.add(rec)
          records ::= tkv -> rec
      }

      val sizeInBytes = DefaultRecordBatch.sizeInBytes(simpleRecords)
      if (sizeInBytes >= fetchMaxBytes) {
        debug(s"sizeIbytes: $sizeInBytes, fetchMaxBytes: $fetchMaxBytes")

        val (head :: tail) = records
        recordBatches :+= (tail.toVector, keyToPrevOffsets)

        simpleRecords.clear()
        simpleRecords.add(head._2)
        records = List(head)
        keyToPrevOffsets = Map()
      }
    }
    recordBatches :+= (records.toVector, keyToPrevOffsets)

    debug(s"${recordBatches.map(x => x._1.size).mkString(",")}")
    recordBatches map { case (records, keyToPrevOffsets) => writeRecords(records, keyToPrevOffsets, topic) }
  }

  private def writeRecords(records: Vector[(TKeyVal, SimpleRecord)], keyToPrevOffsets: Map[Hash, Int], topic: String): Iterable[Int] = {
    try {
      writeLock.lock()

      val topicIdx = topicIndex(topic)

      // write simple records and create index records
      val indexRecords = db.write(topic, records.map(_._2)).foldLeft(Vector[Vector[SimpleRecord]]()) {
        case (indexRecords, (topicPartition, LogAppendResult(appendInfo, Some(ex)))) =>
          error(ex.getMessage, ex) // TODO
          indexRecords

        case (indexRecords, (topicPartition, LogAppendResult(appendInfo, None))) =>
          if (appendInfo.numMessages > 0) {
            val firstOffert = appendInfo.firstOffset.get
            val (lastOffset, idxRecords) = records.foldLeft(firstOffert, Vector[SimpleRecord]()) {
              case ((offset, idxRecords), (TKeyVal(key, value, timestamp), _)) =>
                val hash = Hash(key)
                val hashCode = hash.hashCode
                val indexRecord = new SimpleRecord(intToBytes(hashCode), intToBytes(offset.toInt))

                keyToPrevOffsets.get(hash) match {
                  case Some(prevOffset) => // there is prevOffset, will also remove it (replace it with current one)
                    hashOffsets.replace(hashCode, prevOffset, offset.toInt, topicIdx)
                  case None => // there is none prevOffset
                    hashOffsets.put(hashCode, offset.toInt, topicIdx)
                }
                caches(topicIdx).put(hash, (TVal(value, timestamp), offset.toInt))
                (offset + 1, idxRecords :+ indexRecord)
            }

            assert(appendInfo.lastOffset == lastOffset - 1, s"lastOffset(${appendInfo.lastOffset}) != ${lastOffset - 1}, firstOffset is ${appendInfo.firstOffset}, numOfMessages is ${appendInfo.numMessages}, numRecords is ${records.size}, appendInfo: $appendInfo")

            indexRecords :+ idxRecords
          } else {
            indexRecords
          }
      }

      // write index records
      db.write(indexTopics(topicIdx), indexRecords.flatten) map {
        case (topicPartition, LogAppendResult(appendInfo, Some(ex))) =>
          error(ex.getMessage, ex) // TODO
        case (topicPartition, LogAppendResult(appendInfo, None)) =>
          debug(s"$topic: append index records ${indexRecords.size}")
      }

      indexRecords.map(_.size)
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

  def remove(keys: Seq[Array[Byte]], topic: String): Iterable[Int] = {
    try {
      writeLock.lock()

      val topicIdx = topicIndex(topic)
      caches(topicIdx).remove(keys.map(Hash(_)))

      // create simple records
      val records = keys map { key => key -> new SimpleRecord(key, null) }

      // write simple records and create index records
      val indexRecords = db.write(topic, records.map(_._2)).foldLeft(Vector[Vector[SimpleRecord]]()) {
        case (indexRecords, (topicPartition, LogAppendResult(appendInfo, Some(ex)))) =>
          error(ex.getMessage, ex) // TODO
          indexRecords

        case (indexRecords, (topicPartition, LogAppendResult(appendInfo, None))) =>
          if (appendInfo.numMessages > 0) {
            val firstOffert = appendInfo.firstOffset.get
            val (lastOffset, idxRecords) = records.foldLeft(firstOffert, Vector[SimpleRecord]()) {
              case ((offset, idxRecords), (key, _)) =>
                val hash = Hash(key)
                val hashCode = hash.hashCode
                val indexRecord = new SimpleRecord(intToBytes(hashCode), intToBytes(offset.toInt))

                hashOffsets.put(hashCode, offset.toInt, topicIdx)
                // should also find and remove previous offsets? it will cost 
                // a i/o reading. Not necessary or do it in write
                (offset + 1, idxRecords :+ indexRecord)
            }

            assert(appendInfo.lastOffset == lastOffset - 1, s"lastOffset(${appendInfo.lastOffset}) != ${lastOffset - 1}, firstOffset is ${appendInfo.firstOffset}, numOfMessages is ${appendInfo.numMessages}, numRecords is ${keys.size}, appendInfo: $appendInfo")

            indexRecords :+ idxRecords
          } else {
            indexRecords
          }
      }

      // write index records
      db.write(indexTopics(topicIdx), indexRecords.flatten) map {
        case (topicPartition, LogAppendResult(appendInfo, Some(ex))) =>
          error(ex.getMessage, ex) // TODO
        case (topicPartition, LogAppendResult(appendInfo, None)) =>
          debug(s"$topic: append index records $indexRecords")
      }

      indexRecords.map(_.size)
    } finally {
      writeLock.unlock()
    }
  }

  def iterateOver(fetchOffset: Long, topic: String)(op: (Long, TKeyVal) => Unit) = {
    try {
      readLock.lock()

      db.iterateOver(topic, fetchOffset, fetchMaxBytes)(op)
    } finally {
      readLock.unlock()
    }
  }

  def readOnce(fetchOffset: Long, topic: String)(op: (Long, TKeyVal) => Unit) = {
    try {
      readLock.lock()

      db.readOnce(topic, fetchOffset, fetchMaxBytes)(op)
    } finally {
      readLock.unlock()
    }
  }

}

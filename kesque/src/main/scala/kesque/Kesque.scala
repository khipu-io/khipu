package kesque

import java.io.File
import java.util.Properties
import kafka.server.QuotaFactory.UnboundedQuota
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.record.SimpleRecord
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import scala.collection.mutable

/**
 * Kesque (Kafkaesque)
 *
 * What's Kafkaesque, is when you enter a surreal world in which all your
 * control patterns, all your plans, the whole way in which you have configured
 * your own behavior, begins to fall to pieces, when you find yourself against a
 * force that does not lend itself to the way you perceive the world. You don't
 * give up, you don't lie down and die. What you do is struggle against this
 * with all of your equipment, with whatever you have. But of course you don't
 * stand a chance. That's Kafkaesque. - Frederick R. Karl
 * https://www.nytimes.com/1991/12/29/nyregion/the-essence-of-kafkaesque.html
 *
 * Fill memory:
 * # stress -m 1 --vm-bytes 25G --vm-keep
 */
object Kesque {

  // --- simple test
  def main(args: Array[String]) {
    val khipuPath = new File(classOf[Kesque].getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile.getParentFile
    val configDir = new File(khipuPath, "../src/main/resources")

    val configFile = new File(configDir, "kafka.server.properties")
    val props = org.apache.kafka.common.utils.Utils.loadProps(configFile.getAbsolutePath)
    val kesque = new Kesque(props)
    val topic = "kesque-test"
    val table = kesque.getTable(Array(topic), fetchMaxBytes = 262144, CompressionType.SNAPPY)

    kesque.deleteTable(topic)
    (1 to 2) foreach { i => testWrite(table, topic, i) }
    testRead(table, topic)

    System.exit(0)
  }

  private def testWrite(table: HashKeyValueTable, topic: String, seq: Int) = {
    val kvs = 1 to 100000 map { i =>
      TKeyVal(i.toString.getBytes, (s"value_$i").getBytes)
    }
    table.write(kvs, topic)
  }

  private def testRead(table: HashKeyValueTable, topic: String) {
    val keys = List("1", "2", "3")
    keys foreach { key =>
      val value = table.read(key.getBytes, topic) map (v => new String(v.value))
      println(value) // Some(value_1), Some(value_2), Some(value_3)
    }
  }
}
final class Kesque(props: Properties) {
  private val kafkaServer = KafkaServer.start(props)
  private val replicaManager = kafkaServer.replicaManager

  private val topicToTable = mutable.Map[String, HashKeyValueTable]()

  def getTable(topics: Array[String], fetchMaxBytes: Int = 262144, compressionType: CompressionType = CompressionType.NONE) = {
    topicToTable.getOrElseUpdate(topics.mkString(","), new HashKeyValueTable(topics, this, false, fetchMaxBytes, compressionType))
  }

  def getTimedTable(topics: Array[String], fetchMaxBytes: Int = 262144, compressionType: CompressionType = CompressionType.NONE) = {
    topicToTable.getOrElseUpdate(topics.mkString(","), new HashKeyValueTable(topics, this, true, fetchMaxBytes, compressionType))
  }

  private[kesque] def read(topic: String, fetchOffset: Long, fetchMaxBytes: Int) = {
    val partition = new TopicPartition(topic, 0)
    val partitionData = new PartitionData(fetchOffset, 0L, fetchMaxBytes)

    replicaManager.readFromLocalLog(
      replicaId = 0,
      fetchOnlyFromLeader = true,
      readOnlyCommitted = false,
      fetchMaxBytes = fetchMaxBytes,
      hardMaxBytesLimit = false, // read at lease one message even exceeds the fetchMaxBytes
      readPartitionInfo = List((partition, partitionData)),
      quota = UnboundedQuota,
      isolationLevel = org.apache.kafka.common.requests.IsolationLevel.READ_COMMITTED
    )
  }

  /**
   * Should make sure the size in bytes of batched records is not exceeds the maximum configure value
   */
  private[kesque] def write(topic: String, records: Seq[SimpleRecord], compressionType: CompressionType) = {
    val partition = new TopicPartition(topic, 0)
    //val initialOffset = readerWriter.getLogEndOffset(partition) + 1 // TODO check -1L
    val initialOffset = 0L // TODO is this useful?

    val memoryRecords = kesque.buildRecords(compressionType, initialOffset, records: _*)
    val entriesPerPartition = Map(partition -> memoryRecords)

    replicaManager.appendToLocalLog(
      internalTopicsAllowed = false,
      isFromClient = false,
      entriesPerPartition = entriesPerPartition,
      requiredAcks = 0
    )
  }

  def deleteTable(topic: String) = {
    val partition = new TopicPartition(topic, 0)
    replicaManager.stopReplica(partition, deletePartition = true)
    topicToTable -= topic
  }

  /**
   * @param topic
   * @param fetchOffset
   * @param op: action applied on (offset, key, value)
   */
  private[kesque] def iterateOver(topic: String, fetchOffset: Long = 0L, fetchMaxBytes: Int)(op: (Long, TKeyVal) => Unit) = {
    var offset = fetchOffset
    var nRead = 0
    do {
      readOnce(topic, offset, fetchMaxBytes)(op) match {
        case (n, o) =>
          nRead = n
          offset = o + 1
      }
    } while (nRead > 0)
  }

  /**
   * @param topic
   * @param fetchOffset
   * @param op: action applied on (offset, key, value)
   */
  private[kesque] def readOnce(topic: String, fetchOffset: Long, fetchMaxBytes: Int)(op: (Long, TKeyVal) => Unit) = {
    val (topicPartition, result) = read(topic, fetchOffset, fetchMaxBytes).head
    val recs = result.info.records.records.iterator
    var i = 0
    var lastOffset = fetchOffset
    while (recs.hasNext) {
      val rec = recs.next
      val key = if (rec.hasKey) kesque.getBytes(rec.key) else null
      val value = if (rec.hasValue) kesque.getBytes(rec.value) else null
      val timestamp = rec.timestamp
      val offset = rec.offset
      op(offset, TKeyVal(key, value, timestamp))

      lastOffset = offset
      i += 1
    }

    (i, lastOffset)
  }

  def shutdown() {
    kafkaServer.shutdown()
  }
}

final case class TKeyVal(key: Array[Byte], value: Array[Byte], timestamp: Long = -1L)
final case class TVal(value: Array[Byte], timestamp: Long)

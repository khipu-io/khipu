package khipu.store.datasource

import akka.actor.ActorSystem
import akka.util.ByteString
import java.io.File
import java.util.Arrays
import java.util.Properties
import kesque.HashKeyValueTable
import kesque.HashOffsets
import kesque.Kesque
import kesque.TKeyVal
import kesque.TVal
import khipu.Hash
import khipu.util.Clock
import khipu.util.SimpleMap
import org.apache.kafka.common.record.CompressionType
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Start kafka and create topics:
 *
 * bin/zookeeper-server-start.sh config/zookeeper.properties > logs/zookeeper.log 2>&1 &
 * bin/kafka-server-start.sh config/server.properties > logs/kafka.log 2>&1 &
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic account_500
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic storage_500
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic evmcode
 *
 * To delete topic
 * First set delete.topic.enable=true in server.properties
 * bin/kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --delete --topic sample
 */
object NodeRecord {
  def fromBytes(bytes: Array[Byte]): NodeRecord = {
    val flag = bytes(0)
    val kLenBytes = Array.ofDim[Byte](4)
    val vLenBytes = Array.ofDim[Byte](4)

    System.arraycopy(bytes, 1, kLenBytes, 0, kLenBytes.length)
    System.arraycopy(bytes, 5, vLenBytes, 0, vLenBytes.length)
    val kLen = decode(kLenBytes)
    val vLen = decode(vLenBytes)

    val key = Array.ofDim[Byte](kLen)
    val value = Array.ofDim[Byte](vLen)
    System.arraycopy(bytes, 9, key, 0, key.length)
    System.arraycopy(bytes, 9 + key.length, value, 0, value.length)

    NodeRecord(flag, key, value)
  }

  private def encode(i: Int): Array[Byte] =
    Array((i >>> 24).toByte, ((i << 8) >>> 24).toByte, ((i << 16) >>> 24).toByte, ((i << 24) >>> 24).toByte)

  private def decode(bi: Array[Byte]): Int =
    bi(3) & 0xFF | (bi(2) & 0xFF) << 8 | (bi(1) & 0xFF) << 16 | (bi(0) & 0xFF) << 24
}
final case class NodeRecord(flag: Byte, key: Array[Byte], value: Array[Byte]) {
  import NodeRecord._

  def toBytes = {
    val length = 1 + 4 + 4 + key.length + value.length
    val bytes = Array.ofDim[Byte](length)

    bytes(0) = flag
    val kLenBytes = encode(key.length)
    val vLenBytes = encode(value.length)

    System.arraycopy(kLenBytes, 0, bytes, 1, kLenBytes.length)
    System.arraycopy(vLenBytes, 0, bytes, 5, vLenBytes.length)

    System.arraycopy(key, 0, bytes, 9, key.length)
    System.arraycopy(value, 0, bytes, 9 + key.length, value.length)

    bytes
  }
}

/**
 * Currently, only Trie nodes (accound nodes and storage nodes) will be stored in
 * KesqueDataSource, these nodes, not only are immutable, and also change
 * frequently. And these table needs do compaction
 *
 * evmCode/header/body could be stored in leveldb or kesque, and these table do
 * not need to do compaction, since same key-value pair will be used forever
 *
 * transactions/receipts will be stored in leveldb, since they are not be used
 * during block execution, and are a bit too many.
 */
object KesqueDataSource {
  val account = "account"
  val storage = "storage"
  val evmcode = "evmcode"
  val header = "header"
  val body = "body"
  val td = "td" // total difficulty
  val receipts = "receipts"

  def kafkaProps(system: ActorSystem) = {
    val props = new Properties()
    system.settings.config.getConfig("khipu.kafka").entrySet().asScala.foreach { entry =>
      props.put(entry.getKey, entry.getValue.unwrapped.toString)
    }
    props
  }

  // --- simple test
  def main(args: Array[String]) {
    val khipuPath = new File(classOf[KesqueDataSource].getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile.getParentFile
    //val configDir = new File(khipuPath, "conf")
    val configDir = new File(khipuPath, "../src/universal/conf")

    val configFile = new File(configDir, "kafka.server.properties")
    val props = org.apache.kafka.common.utils.Utils.loadProps(configFile.getAbsolutePath)
    val db = new Kesque(props)

    //findData(db, "storage_500", "a6b291fa802416bace4e79f635cedbd1a1a333f5b664153844c3035b60e0bf6b")
    //dump(db, 0L)
    testRead(db, evmcode, 102400)

    System.exit(0)
  }

  private def dump(db: Kesque, fetchOffset: Long) = {
    val src = "evmcode"
    val tgt = "evmcode_new"
    db.deleteTable(tgt)
    val tableSrc = db.getTable(Array(src), fetchMaxBytes = 102400, CompressionType.NONE)
    val tableTgt = db.getTable(Array(tgt), fetchMaxBytes = 102400, CompressionType.NONE)

    val hashOffsets = new HashOffsets(200)

    var offset = fetchOffset
    var nRead = 0
    do {
      val records = mutable.ArrayBuffer[(Long, Array[Byte], Array[Byte])]()
      tableSrc.readOnce(offset, src) {
        case (offset, TKeyVal(k, v, t)) =>
          val NodeRecord(flag, key, value) = NodeRecord.fromBytes(v)
          val hash = Hash(key)
          hashOffsets.put(hash.hashCode, offset.toInt, 0) // TODO

          records += ((offset, key, value))
      } match {
        case (n, o) =>
          println("nRecs read: " + n + ", lastOffset: " + o)

          val sorted = records.sortBy(x => x._1).map(x => TKeyVal(x._2, x._3))
          val writeResults = tableTgt.write(sorted, tgt)
          println(writeResults) // TODO check exceptions

          nRead = n
          offset = o + 1
      }
    } while (nRead > 0)

    println("hashOffsets size: " + hashOffsets.size)
  }

  /**
   * Disable kernel page cache when test:
   * # while true;  do echo 1 > /proc/sys/vm/drop_caches;echo clean cache ok; sleep 1; done
   */
  private def testRead(db: Kesque, topic: String, fetchMaxBytes: Int = 1024000) = {
    val table = db.getTable(Array(topic), fetchMaxBytes, CompressionType.NONE)

    val records = new mutable.HashMap[Hash, ByteString]()
    table.iterateOver(0, topic) {
      case (offset, TKeyVal(key, value, timestamp)) =>
        //print(s"$offset - ${Hash(key).hexString},")
        val hash = Hash(key)
        if (records.contains(hash)) {
          val existed = records.get(hash).get
          println(s"$offset - ${hash.hexString} with existed value, is equal? ${existed == ByteString(value)} new: ${ByteString(value)}, old: ${existed}")
        } else {
          records += (hash -> ByteString(value))
        }
    }

    println("key size: " + records.size)

    val reportCount = 1000
    var start = System.nanoTime
    var count = 0
    records foreach {
      case (key, value) =>
        table.read(key.bytes, topic) match {
          case Some(x) =>
            count += 1
            if (count % reportCount == 0) {
              println(s"rate: ${reportCount / ((System.nanoTime - start) / 1000000000.0)}")
              start = System.nanoTime
            }
          case None => println(s"not found key ${key}")
        }
    }
    println(s"Found: $count, keys: ${records.size}")
  }

  def findData(db: Kesque, topic: String, keyInHex: String) = {
    val lostKey = khipu.hexDecode(keyInHex)

    var prevValue: Option[Array[Byte]] = None
    val table = db.getTable(Array(topic))
    table.iterateOver(0, topic) {
      case (offset, TKeyVal(key, value, timestamp)) =>
        if (Arrays.equals(key, lostKey)) {
          val keyHash = Hash(lostKey).hashCode
          println(s"Found $keyInHex at offset $offset, key hash is $keyHash")
          prevValue map { x =>
            if (Arrays.equals(x, value)) {
              println("Same value as previous")
            } else {
              println("Diff value to previous")
            }
          }
          prevValue = Some(value)
          table.read(lostKey, topic) match {
            case Some(value) => println(s"value: ${value.value.length}")
            case None        => println("Can not read value")
          }
        }
    }
  }

}
final class KesqueDataSource(val table: HashKeyValueTable, val topic: String)(implicit system: ActorSystem) extends SimpleMap[Hash, TVal, KesqueDataSource] {
  import KesqueDataSource._

  private var _currWritingBlockNumber: Long = _
  def setWritingBlockNumber(writingBlockNumber: Long) {
    this._currWritingBlockNumber = writingBlockNumber
  }

  val clock = new Clock()

  def get(key: Hash): Option[TVal] = {
    val start = System.nanoTime
    val value = table.read(key.bytes, topic)
    clock.elapse(System.nanoTime - start)
    value
  }

  def update(toRemove: Set[Hash], toUpsert: Map[Hash, TVal]): KesqueDataSource = {
    // TODO what's the meaning of remove a node? sometimes causes node not found
    //table.remove(toRemove.map(_.bytes).toList)
    table.write(toUpsert.map { case (key, value) => TKeyVal(key.bytes, value.value, _currWritingBlockNumber) }, topic)
    this
  }

  override def updatePost(toRemove: Set[Hash], toUpsert: Map[Hash, TVal]): KesqueDataSource = {
    // TODO what's the meaning of remove a node? sometimes causes node not found
    //table.remove(toRemove.map(_.bytes).toList)
    table.writePost(toUpsert.map { case (key, value) => TKeyVal(key.bytes, value.value, _currWritingBlockNumber) }, topic)
    this
  }

  def cacheHitRate = table.cacheHitRate(topic)
  def cacheReadCount = table.cacheReadCount(topic)
  def resetCacheHitRate() = table.resetCacheHitRate(topic)
}

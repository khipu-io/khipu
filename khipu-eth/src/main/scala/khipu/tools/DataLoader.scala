package khipu.tools

import akka.actor.ActorSystem
import khipu.Hash
import khipu.domain.Account
import khipu.domain.Blockchain
import khipu.rlp
import khipu.store.datasource.KesqueDataSource
import khipu.store.datasource.NodeRecord
import khipu.trie
import khipu.trie.Node
import khipu.trie.BranchNode
import khipu.trie.ByteArraySerializable
import khipu.trie.ExtensionNode
import khipu.trie.LeafNode
import khipu.trie.MerklePatriciaTrie.MPTException
import khipu.vm.UInt256
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import scala.collection.mutable
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Start kafka and create topics:
 *
 * bin/zookeeper-server-start.sh config/zookeeper.properties > logs/zookeeper.log 2>&1 &
 * bin/kafka-server-start.sh config/server.properties > logs/kafka.log 2>&1 &
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic account
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic storage
 * bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic evmcode
 *
 * To delete topic
 * First set delete.topic.enable=true in server.properties
 * bin/kafka-run-class.sh kafka.admin.TopicCommand --zookeeper localhost:2181 --delete --topic sample
 */
final class DataLoader(blockchain: Blockchain)(implicit system: ActorSystem) {
  private val kafkaProps = KesqueDataSource.kafkaProps(system)

  private val nodeKeyValueStorage = blockchain.storages.accountNodeStorageFor(Some(0)) // // TODO if want to load from leveldb, rewrite a storage
  private val evmcodeStorage = blockchain.storages.evmCodeStorage

  private val accountTopic = "account"
  private val storageTopic = "storage"
  private val evmcodeTopic = "evmcode"

  private val producer = new KafkaProducer[Array[Byte], Array[Byte]](kafkaProps)

  private val accountReader = new NodeReader[Account](Account.accountSerializer, accountTopic, isExport = false)
  private val storageReader = new NodeReader[UInt256](trie.rlpUInt256Serializer, storageTopic, isExport = false)
  private val isExportEvmCode = false

  private val loadedEvmcodeKeys = mutable.HashSet[Hash]()

  private val start = System.currentTimeMillis
  private var nodeCount = 0
  private var accountCount = 0
  private var storageCount = 0
  private var evmcodeCount = 0

  def loadFromLevelDb(blockNumber: Long) {
    println(s"loading nodes of #$blockNumber")
    blockchain.getBlockHeaderByNumber(blockNumber) map { blockHeader =>
      val stateRoot = blockHeader.stateRoot.bytes
      accountReader.getNode(stateRoot) map accountReader.processNode
    }
  }

  final class NodeReader[V](vSerializer: ByteArraySerializable[V], exportTopic: String, isExport: Boolean = false) {
    private val loadedKeys = mutable.HashSet[Hash]()

    private def toEntity(valueBytes: Array[Byte]) = vSerializer.fromBytes(valueBytes)

    def processNode(node: Node) {
      node match {
        case LeafNode(key, value) => entityGot(toEntity(value))
        case ExtensionNode(shardedKey, next) =>
          next match {
            case Left(nodeId) => getNode(nodeId) map processNode
            case Right(node)  => processNode(node)
          }
        case BranchNode(children, terminator) =>
          children.map {
            case Some(Left(nodeId)) => getNode(nodeId) map processNode
            case Some(Right(node))  => processNode(node)
            case None               =>
          }

          terminator match {
            case Some(value) => entityGot(toEntity(value))
            case None        =>
          }
      }
    }

    def getNode(nodeId: Array[Byte]): Option[Node] = {
      val encodedOpt = if (nodeId.length < 32) {
        Some(nodeId)
      } else {
        val hash = Hash(nodeId)
        if (!loadedKeys.contains(hash)) {
          loadedKeys += hash
          nodeKeyValueStorage.get(hash) match {
            case Some(bytes) =>
              if (isExport) {
                producer.send(new ProducerRecord(exportTopic, NodeRecord(0, hash.bytes, bytes).toBytes))
              }

              nodeCount += 1
              val elapsed = (System.currentTimeMillis - start) / 1000
              val speed = nodeCount / math.max(1, elapsed)
              println(s"${java.time.LocalTime.now} $nodeCount cache ${blockchain.storages.cacheSize} ${speed}/s")

              Some(bytes)
            case None => throw MPTException(s"Node not found ${khipu.toHexString(nodeId)}, trie is inconsistent")
          }
        } else {
          None
        }
      }

      encodedOpt map (encoded => rlp.decode[Node](encoded)(Node.nodeDec))
    }

    private def entityGot(entity: V) = {
      entity match {
        case Account(_, _, stateRoot, codeHash) =>
          accountCount += 1
          println(s"got account $accountCount")
          if (stateRoot != Account.EmptyStorageRootHash) {
            storageReader.getNode(stateRoot.bytes) map storageReader.processNode
          }

          if (codeHash != Account.EmptyCodeHash && !loadedEvmcodeKeys.contains(codeHash)) {
            loadedEvmcodeKeys += codeHash
            evmcodeStorage.get(codeHash) map { evmcode =>
              evmcodeCount += 1
              println(s"got evmcode $evmcodeCount")

              if (isExportEvmCode) {
                producer.send(new ProducerRecord(evmcodeTopic, NodeRecord(0, codeHash.bytes, evmcode.toArray).toBytes))
              }
            }
          }

        case _: UInt256 =>
          storageCount += 1
          println(s"got storage $storageCount")

        case _ =>
      }
    }
  }
}

final class NodeDump(blockchain: Blockchain)(implicit system: ActorSystem) {
  private val kafkaProps = KesqueDataSource.kafkaProps(system)

  private val fromTopic = "account"
  private val toTopic = "account_500"
  private val loadedKeys = mutable.HashSet[Hash]()

  def dump() {
    dumpTopic(fromTopic, toTopic)
  }

  private def dumpTopic(fromTopic: String, toTopic: String) {
    val start = System.currentTimeMillis

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaProps)
    val partition = new TopicPartition(fromTopic, 0)
    consumer.assign(java.util.Arrays.asList(partition))
    consumer.seekToBeginning(java.util.Arrays.asList(partition))

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](kafkaProps)

    var count = 0
    var nPolled = 0
    var retries = 0
    while (nPolled > 0 || retries < 10) {
      nPolled = poll(consumer, producer, fromTopic, toTopic)

      if (nPolled > 0) {
        retries = 0 // reset
      } else {
        retries += 1
      }

      count += nPolled
      println(s"read $fromTopic ${count} in ${(System.currentTimeMillis - start) / 1000.0}s, nPolled $nPolled")
    }

    println(s"loaded keys ${loadedKeys.size} in ${(System.currentTimeMillis - start) / 1000.0}s")

    benchmarkLoadFromLevelDb()
  }

  private def poll(consumer: KafkaConsumer[Array[Byte], Array[Byte]], producer: KafkaProducer[Array[Byte], Array[Byte]], fromTopic: String, toTopic: String, isExport: Boolean = false): Int = {
    try {
      var count = 0
      val records = consumer.poll(1000)
      val partitions = records.partitions.iterator
      while (partitions.hasNext) {
        val partition = partitions.next()
        val partitionRecords = records.records(partition)
        val parRecords = partitionRecords.iterator
        while (parRecords.hasNext) {
          val record = parRecords.next()
          count += 1
          val topic = record.topic
          if (topic == fromTopic) {
            val NodeRecord(flag, key, value) = NodeRecord.fromBytes(record.value)
            val hash = Hash(key)

            if (!loadedKeys.contains(hash)) {
              loadedKeys += hash
              if (isExport) {
                producer.send(new ProducerRecord(toTopic, NodeRecord(flag, key, value).toBytes))
              }
            }
          }
        }
      }

      count
    } catch {
      case e: Throwable =>
        println(e, e.getMessage)
        0
    }
  }

  private val nodeKeyValueStorage = blockchain.storages.accountNodeStorageFor(Some(0)) // TODO if want to benchmark leveldb, rewrite a storage
  private def benchmarkLoadFromLevelDb() {
    val keys = loadedKeys.toArray
    val random = new Random()
    def randomOffset() = {
      val lo = 0
      val hi = keys.length - 1
      random.nextInt(hi - lo) + lo
    }

    val start = System.currentTimeMillis
    var i = 0
    while (i < keys.length) {
      val key = keys(i)
      val node = nodeKeyValueStorage.get(key)
      val elapsed = (System.currentTimeMillis - start) / 1000
      val speed = i / math.max(1, elapsed)
      println(s"${java.time.LocalTime.now} $i ${speed}/s node $node")
      i += 1
    }
  }
}

final class DataFinder()(implicit system: ActorSystem) {
  private val kafkaProps = KesqueDataSource.kafkaProps(system)

  private val lostKeys = Set(
    "1c4d41d28dc7083a1ccf8965db452a09c5b9838322e5ec22be77ba1fc7353590"
  ).map(khipu.hexDecode).map(Hash(_))

  def find() {
    find("storage_500")
  }

  private def find(fromTopic: String) {
    val start = System.currentTimeMillis

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](kafkaProps)
    val partition = new TopicPartition(fromTopic, 0)
    consumer.assign(java.util.Arrays.asList(partition))
    consumer.seekToBeginning(java.util.Arrays.asList(partition))

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](kafkaProps)

    var count = 0
    var nPolled = 0
    var retries = 0
    while (nPolled > 0 || retries < 10) {
      nPolled = poll(consumer, producer, fromTopic)

      if (nPolled > 0) {
        retries = 0 // reset
      } else {
        retries += 1
      }

      count += nPolled
      println(s"read $fromTopic ${count} in ${(System.currentTimeMillis - start) / 1000.0}s, nPolled $nPolled")
    }

    println(s"loaded keys $count in ${(System.currentTimeMillis - start) / 1000.0}s")
  }

  private def poll(consumer: KafkaConsumer[Array[Byte], Array[Byte]], producer: KafkaProducer[Array[Byte], Array[Byte]], fromTopic: String): Int = {
    try {
      var count = 0
      val records = consumer.poll(1000)
      val partitions = records.partitions.iterator
      while (partitions.hasNext) {
        val partition = partitions.next()
        val partitionRecords = records.records(partition)
        val parRecords = partitionRecords.iterator
        while (parRecords.hasNext) {
          val record = parRecords.next()
          count += 1
          val topic = record.topic
          if (topic == fromTopic) {
            val NodeRecord(flag, key, value) = NodeRecord.fromBytes(record.value)
            val hash = Hash(key)

            if (lostKeys.contains(hash)) {
              println(s"found: $hash")
            }
          }
        }
      }

      count
    } catch {
      case e: Throwable =>
        println(e, e.getMessage)
        0
    }
  }
}

//final class BlockDump(blockchain: Blockchain, db: Kesque)(implicit system: ActorSystem) {
//  private val numberMappingStorage = blockchain.storages.blockNumberMappingStorage
//  private val headerStorage = blockchain.storages.blockHeaderStorage
//  private val bodyStorage = blockchain.storages.blockBodyStorage
//
//  def dump() {
//    println("start dump blocks ...")
//
//    var start = System.currentTimeMillis
//    val blockTable = db.getTable(Array("header", "body"), 1024000)
//
//    //val headerTable = db.getTable("header")
//    //val bodyTable = db.getTable("body")
//
//    var blockNumber = 0L
//    var continue = true
//    while (continue) {
//      print(s"$blockNumber,")
//      continue = numberMappingStorage.get(blockNumber) match {
//        case Some(blockHash) =>
//          headerStorage.get(blockHash) match {
//            case Some(header) =>
//              import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
//              blockTable.write(List(Record(blockHash.bytes, header.toBytes, blockNumber)), "header")
//            case None =>
//          }
//          bodyStorage.get(blockHash) match {
//            case Some(body) =>
//              import khipu.network.p2p.messages.PV62.BlockBody.BlockBodyDec
//              blockTable.write(List(Record(blockHash.bytes, body.toBytes, blockNumber)), "body")
//            case None =>
//          }
//          true
//        case None =>
//          println(s"none for block #$blockNumber")
//          false
//      }
//
//      if (blockNumber % 1000 == 0) {
//        println(s"\ndumped up to $blockNumber in ${(System.currentTimeMillis - start) / 1000.0}s")
//        start = System.currentTimeMillis
//      }
//
//      blockNumber += 1
//    }
//
//  }
//
//}
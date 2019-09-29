package khipu.storage

import akka.actor.ActorSystem
import akka.event.LogSource
import akka.event.Logging
import java.io.File
import java.nio.ByteBuffer
import kesque.Kesque
import kesque.KesqueIndexRocksdb
import khipu.Hash
import khipu.Khipu
import khipu.TKeyVal
import khipu.TVal
import khipu.DataWord
import khipu.config.DbConfig
import khipu.config.KhipuConfig
import khipu.config.RocksdbConfig
import khipu.crypto
import khipu.domain.Account
import khipu.rlp
import khipu.storage.datasource.KesqueBlockDataSource
import khipu.storage.datasource.KesqueNodeDataSource
import khipu.trie
import khipu.trie.BranchNode
import khipu.trie.ByteArraySerializable
import khipu.trie.ExtensionNode
import khipu.trie.LeafNode
import khipu.trie.Node
import khipu.util
import org.lmdbjava.Env
import scala.collection.mutable

object KesqueNodeCompactor {
  implicit lazy val system = ActorSystem("khipu")
  import system.dispatcher

  private val FETCH_MAX_BYTES_IN_BACTH = 50 * 1024 * 1024 // 52428800, 50M

  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName
    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }

  object NodeReader {
    final case class TNode(node: Node, blockNumber: Long)
  }
  class NodeReader[V](topic: String, nodeDataSource: KesqueNodeDataSource)(vSerializer: ByteArraySerializable[V]) {
    import NodeReader._
    private val log = Logging(system, this)

    private val start = System.nanoTime
    private var nodeCount = 0
    private var entityCount = 0

    /** override me to define the behavior */
    protected def entityGot(entity: V, blockNumber: Long) {}
    protected def nodeGot(kv: TKeyVal) {}

    private def toEntity(valueBytes: Array[Byte]): V = vSerializer.fromBytes(valueBytes)

    private def processEntity(entity: V, blockNumber: Long) = {
      entityCount += 1
      if (entityCount % 10000 == 0) {
        log.debug(s"[comp] got $topic entities $entityCount, at #$blockNumber")
      }

      entityGot(entity, blockNumber)
    }

    def processNode(tnode: TNode) {

      tnode match {
        case TNode(LeafNode(key, value), blockNumber) => processEntity(toEntity(value), blockNumber)
        case TNode(ExtensionNode(shardedKey, next), blockNumber) =>
          next match {
            case Left(nodeId) => getNode(nodeId, blockNumber) map processNode
            case Right(node)  => processNode(TNode(node, blockNumber))
          }
        case TNode(BranchNode(children, terminator), blockNumber) =>
          children.map {
            case Some(Left(nodeId)) => getNode(nodeId, blockNumber) map processNode
            case Some(Right(node))  => processNode(TNode(node, blockNumber))
            case None               =>
          }

          terminator match {
            case Some(value) => processEntity(toEntity(value), blockNumber)
            case None        =>
          }
      }
    }

    def getNode(key: Array[Byte], blockNumber: Long): Option[TNode] = {
      val encodedOpt = if (key.length < 32) {
        Some(key, blockNumber)
      } else {
        nodeDataSource.getWithOffset(Hash(key), notCache = true) match {
          case Some(TVal(value, offset)) =>
            nodeCount += 1
            if (nodeCount % 10000 == 0) {
              val elapsed = (System.nanoTime - start) / 1000000000
              val speed = nodeCount / math.max(1, elapsed)
              println(s"[comp] $topic nodes $nodeCount $speed/s, at #$blockNumber, table size ${nodeDataSource.count}")
            }

            nodeGot(TKeyVal(key, value, offset))
            Some(value, blockNumber)

          case None =>
            println(s"$topic Node not found ${khipu.toHexString(key)}, trie is inconsistent")
            None
        }
      }

      encodedOpt map {
        case (encoded, blockNumber) => TNode(rlp.decode[Node](encoded)(Node.nodeDec), blockNumber)
      }
    }

  }

  final class NodeWriter(topic: String, nodeDataSource: KesqueNodeDataSource) {
    private val buf = new mutable.ArrayBuffer[TKeyVal]()

    private var _maxOffset = -1L // kafka offset starts from 0
    def maxOffset = _maxOffset

    /**
     * Should flush() after all kv are written.
     */
    def write(kv: TKeyVal) {
      if (kv ne null) {
        buf += kv
        if (buf.size > 100) { // keep the batched size around 4096 (~ 32*100 bytes)
          flush()
        }
      }
    }

    def flush() {
      val kvs = buf map {
        case TKeyVal(_, value, offset) =>
          _maxOffset = math.max(_maxOffset, offset)
          val key = crypto.kec256(value)
          Hash(key) -> value
      }
      nodeDataSource.update(Nil, kvs)

      buf.clear()
    }
  }

  /**
   * Used for testing only
   */
  private def initTablesBySelf() = {
    val khipuPath = new File(classOf[Khipu].getProtectionDomain.getCodeSource.getLocation.toURI).getParentFile.getParentFile.getParentFile
    val configDir = new File(khipuPath, "src/universal/conf")
    val configFile = new File(configDir, "kafka.server.properties")
    val kafkaProps = {
      val props = org.apache.kafka.common.utils.Utils.loadProps(configFile.getAbsolutePath)
      props.put("log.dirs", KhipuConfig.kesqueDir)
      props
    }
    val kesque = new Kesque(kafkaProps)

    val config = KhipuConfig.config
    val datadir = KhipuConfig.datadir
    println(s"kafka.server.properties: $configFile, datadir: $datadir")
    val rocksdbConfig = new RocksdbConfig(datadir, config.getConfig("db").getConfig("rocksdb"))

    val accountDataSource = new KesqueNodeDataSource(DbConfig.account, kesque, new KesqueIndexRocksdb(rocksdbConfig, DbConfig.account, useShortKey = true), cacheSize = 1000)
    val storageDataSource = new KesqueNodeDataSource(DbConfig.storage, kesque, new KesqueIndexRocksdb(rocksdbConfig, DbConfig.storage, useShortKey = true), cacheSize = 1000)
    val evmcodeDataSource = new KesqueNodeDataSource(DbConfig.evmcode, kesque, new KesqueIndexRocksdb(rocksdbConfig, DbConfig.evmcode, useShortKey = true), cacheSize = 1000)
    val blockHeaderDataSource = new KesqueBlockDataSource(DbConfig.header, kesque, cacheSize = 1000)

    (kesque, accountDataSource, storageDataSource, blockHeaderDataSource, rocksdbConfig)
  }

  // --- simple test
  def main(args: Array[String]) {
    val (kesque, accountDataSource, storageDataSource, blockHeaderStorage, rocksdbConfig) = initTablesBySelf()
    //val storages = serviceBoard.storages
    //val kesque = storages.asInstanceOf[KesqueDataSources].kesque
    //val accountDataSource = storages.accountNodeDataSource.asInstanceOf[KesqueNodeDataSource]
    //val storageDataSource = storages.storageNodeDataSource.asInstanceOf[KesqueNodeDataSource]
    //val blockHeaderStorage = storages.blockHeaderStorage
    //val rocksdbConfig = storages.asInstanceOf[KesqueRocksdbDataSources].rocksdbConfig

    val compactor = new KesqueNodeCompactor(kesque, accountDataSource, storageDataSource, rocksdbConfig, blockHeaderStorage, 8445282)
    compactor.start()
  }
}
final class KesqueNodeCompactor(
    kesque:             Kesque,
    accountDataSource:  KesqueNodeDataSource,
    storageDataSource:  KesqueNodeDataSource,
    rocksdbConfig:      RocksdbConfig,
    blockHeaderStorage: KesqueBlockDataSource,
    blockNumber:        Long
) {
  import KesqueNodeCompactor._

  private val log = Logging(system, this)

  private val targetAccountDataSource = new KesqueNodeDataSource(DbConfig.account + "~", kesque, new KesqueIndexRocksdb(rocksdbConfig, DbConfig.account + "~", useShortKey = true), cacheSize = 100)
  private val targetStorageDataSource = new KesqueNodeDataSource(DbConfig.storage + "~", kesque, new KesqueIndexRocksdb(rocksdbConfig, DbConfig.storage + "~", useShortKey = true), cacheSize = 100)
  private val targetEvmcodeDataSource = new KesqueNodeDataSource(DbConfig.evmcode + "~", kesque, new KesqueIndexRocksdb(rocksdbConfig, DbConfig.evmcode + "~", useShortKey = true), cacheSize = 100)

  private val storageWriter = new NodeWriter(DbConfig.storage, targetStorageDataSource)
  private val accountWriter = new NodeWriter(DbConfig.account, targetAccountDataSource)

  private val storageReader = new NodeReader[DataWord](DbConfig.storage, storageDataSource)(trie.rlpDataWordSerializer) {
    override def nodeGot(kv: TKeyVal) {
      storageWriter.write(kv)
    }
  }

  private val accountReader = new NodeReader[Account](DbConfig.account, accountDataSource)(Account.accountSerializer) {
    override def entityGot(account: Account, blocknumber: Long) {
      // try to extracted storage node hash
      if (account.stateRoot != Account.EMPTY_STATE_ROOT_HASH) {
        storageReader.getNode(account.stateRoot.bytes, blocknumber) map storageReader.processNode
      }
    }

    override def nodeGot(kv: TKeyVal) {
      accountWriter.write(kv)
    }
  }

  def start() {
    //loadSnaphot()
    //stopWorld(() => true)
    //postAppend()
  }

  private def loadSnaphot() {
    import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._
    val start = System.nanoTime
    println(s"[comp] loading nodes of #$blockNumber")
    for {
      header <- blockHeaderStorage.get(blockNumber).map(_.toBlockHeader)
    } {
      val stateRoot = header.stateRoot.bytes
      accountReader.getNode(stateRoot, blockNumber) map accountReader.processNode
    }
    storageWriter.flush()
    accountWriter.flush()
    val elapsed = (System.nanoTime - start) / 1000000000
    println(s"[comp] all nodes loaded of #$blockNumber in ${elapsed}s")
  }

  def stopWorld(stop: () => Boolean): Boolean = {
    stop()
  }

  /**
   * should stop world during postAppend()
   */
  private def postAppend() {
    val start = System.nanoTime

    val storageTask = new Thread {
      override def run() {
        println(s"[comp] storage post append from offset ${storageWriter.maxOffset + 1} ...")
        val start = System.nanoTime
        var offset = storageWriter.maxOffset + 1
        var nRead = 0
        var count = 0
        do {
          val (lastOffset, recs) = storageDataSource.readBatch(DbConfig.account, offset, FETCH_MAX_BYTES_IN_BACTH)
          recs foreach storageWriter.write
          nRead = recs.length
          offset = lastOffset + 1

          count += nRead
          val elapsed = (System.nanoTime - start) / 1000000000
          val speed = count / math.max(1, elapsed)
          println(s"[comp] storage nodes $count $speed/s, at #$blockNumber, table size ${storageDataSource.count}")
        } while (nRead > 0)

        storageWriter.flush()
        println(s"[comp] storage post append done.")
      }
    }

    val accountTask = new Thread {
      override def run() {
        println(s"[comp] account post append from offset ${accountWriter.maxOffset + 1} ...")
        val start = System.nanoTime
        var offset = accountWriter.maxOffset + 1
        var nRead = 0
        var count = 0
        do {
          val (lastOffset, recs) = accountDataSource.readBatch(DbConfig.account, offset, FETCH_MAX_BYTES_IN_BACTH)
          recs foreach accountWriter.write
          nRead = recs.length
          offset = lastOffset + 1

          count += nRead
          val elapsed = (System.nanoTime - start) / 1000000000
          val speed = count / math.max(1, elapsed)
          println(s"[comp] account nodes $count $speed/s, at #$blockNumber, table size ${accountDataSource.count}")
        } while (nRead > 0)

        accountWriter.flush()
        println(s"[comp] account post append done.")
      }
    }

    storageTask.start
    accountTask.start

    storageTask.join
    accountTask.join

    val elapsed = (System.nanoTime - start) / 1000000000
    println(s"[comp] post append done in ${elapsed}s.")
  }
}
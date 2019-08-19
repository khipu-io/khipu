package khipu.tools

import akka.actor.ActorSystem
import akka.event.LogSource
import akka.event.Logging
import khipu.Hash
import khipu.DataWord
import khipu.config.DbConfig
import khipu.domain.Account
import khipu.rlp
import khipu.service.ServiceBoard
import khipu.store.NodeStorage
import khipu.store.Storages.DefaultStorages
import khipu.trie
import khipu.trie.BranchNode
import khipu.trie.ByteArraySerializable
import khipu.trie.ExtensionNode
import khipu.trie.LeafNode
import khipu.trie.Node
import scala.collection.mutable

object DataChecker {
  implicit lazy val system = ActorSystem("khipu")
  import system.dispatcher
  lazy val serviceBoard = ServiceBoard(system)
  lazy val dbConfig = serviceBoard.dbConfig
  lazy val storages = serviceBoard.storages

  implicit val logSource: LogSource[AnyRef] = new LogSource[AnyRef] {
    def genString(o: AnyRef): String = o.getClass.getName
    override def getClazz(o: AnyRef): Class[_] = o.getClass
  }

  object NodeReader {
    final case class TNode(node: Node, blockNumber: Long)
  }
  class NodeReader[V](topic: String, nodeStorage: NodeStorage)(vSerializer: ByteArraySerializable[V]) {
    import NodeReader._
    private val log = Logging(system, this)

    var nodeCount = 0
    private var entityCount = 0
    private val start = System.nanoTime

    /** override me to define the behavior */
    protected def entityGot(entity: V, blockNumber: Long) {}
    protected def nodeGot(k: Array[Byte], v: Array[Byte]) {}

    private def toEntity(valueBytes: Array[Byte]): V = vSerializer.fromBytes(valueBytes)

    private def processEntity(entity: V, blockNumber: Long) = {
      entityCount += 1
      if (entityCount % 10000 == 0) {
        log.info(s"got $topic entities $entityCount, at #$blockNumber")
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
        nodeStorage.get(Hash(key)) match {
          case Some(bytes) =>
            nodeCount += 1
            if (nodeCount % 10000 == 0) {
              val elapsed = (System.nanoTime - start) / 1000000000
              val speed = nodeCount / math.max(1, elapsed)
              log.info(s"$topic nodes $nodeCount $speed/s")
            }

            nodeGot(key, bytes)
            Some(bytes, blockNumber)

          case None =>
            log.error(s"$topic Node not found ${khipu.toHexString(key)}, trie is inconsistent")
            None
        }
      }

      encodedOpt map {
        case (encoded, blockNumber) => TNode(rlp.decode[Node](encoded)(Node.nodeDec), blockNumber)
      }
    }

  }

  def main(args: Array[String]) {
    val lostKeys = Set( //"6def56fedd6eb859547b1b5759f62a94162541ea7aad120e6ecd1e76b0cd8af3",
    //"1c61d8677af71ddc0aaf835d26c257eccc68d44506f853eef1962b5fb8ec369a",
    //"a93fb7195e99fecd525faa33aae35903387d6038ec7afd7ec43d7a5cb9cf4686"
    ).map(khipu.hexDecode).map(Hash(_))

    val stateNode = None // Some(khipu.hexDecode("c51d801ae31c788f43ed3e09dca04876825c58daea01a531a8377c57748f012f"))
    new DataChecker(storages, 8044437, stateNode, lostKeys).loadSnaphot()
  }

}
class DataChecker(storages: DefaultStorages, blockNumber: Long, stateRoot: Option[Array[Byte]], checkList: Set[Hash]) {
  import DataChecker._
  private val log = Logging(system, this)

  private val blockHeaderStorage = storages.blockHeaderStorage
  private val accountNodeStorage = storages.accountNodeStorage
  private val storageNodeStorage = storages.storageNodeStorage
  private val evmcodeStorage = storages.evmcodeStorage

  private val storageReader = new NodeReader[DataWord](DbConfig.storage, storageNodeStorage)(trie.rlpDataWordSerializer) {
    override def nodeGot(k: Array[Byte], v: Array[Byte]) {
      if (checkList.contains(Hash(k))) {
        log.info(s"found ${Hash(k)}")
      }
    }
  }

  private val accountReader = new NodeReader[Account](DbConfig.account, accountNodeStorage)(Account.accountSerializer) {
    override def entityGot(account: Account, blocknumber: Long) {
      // try to extracted storage node hash
      if (account.stateRoot != Account.EMPTY_STATE_ROOT_HASH) {
        storageReader.getNode(account.stateRoot.bytes, blocknumber) map storageReader.processNode
      }
      if (account.codeHash != Account.EMPTY_CODE_HASH) {
        evmcodeStorage.get(account.codeHash) match {
          case None => log.error(s"evmcode not found ${account.codeHash}, trie is inconsistent")
          case _    =>
        }
      }
    }

    override def nodeGot(k: Array[Byte], v: Array[Byte]) {
      if (checkList.contains(Hash(k))) {
        log.info(s"found ${Hash(k)}")
      }
    }
  }

  def loadSnaphot() {
    log.info(s"loading nodes of #$blockNumber")
    blockHeaderStorage.get(blockNumber) map (_.stateRoot.bytes) orElse stateRoot foreach { root =>
      log.info(s"stateRoot: ${Hash(root)}")
      accountReader.getNode(root, blockNumber) map accountReader.processNode
    }
    val totalNodeCount = accountReader.nodeCount + storageReader.nodeCount
    log.info(s"all nodes loaded of #$blockNumber: total $totalNodeCount, account ${accountReader.nodeCount}, storage ${storageReader.nodeCount}")
  }
}

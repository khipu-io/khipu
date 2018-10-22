package khipu.blockchain

import akka.util.ByteString
import java.util.Arrays
import khipu.Command
import khipu.Hash
import khipu.crypto
import khipu.domain.Account
import khipu.domain.BlockHeader
import khipu.domain.Receipt
import khipu.network.p2p.Message
import khipu.network.p2p.MessageSerializable
import khipu.network.p2p.messages.PV62
import khipu.network.p2p.messages.PV63

package object sync {
  object NodeHash {
    val StateMptNode = 0.toByte
    val ContractStorageMptNode = 1.toByte
    val StorageRoot = 2.toByte
    val Evmcode = 3.toByte
  }
  sealed trait NodeHash extends Hash.I {
    def tpe: Byte

    final def toHash: Hash = Hash(bytes)

    final override def equals(any: Any) = {
      any match {
        case that: NodeHash => (this eq that) || this.tpe == that.tpe && Arrays.equals(this.bytes, that.bytes)
        case _              => false
      }
    }
  }
  final case class StateMptNodeHash(bytes: Array[Byte]) extends NodeHash { def tpe = NodeHash.StateMptNode }
  final case class ContractStorageMptNodeHash(bytes: Array[Byte]) extends NodeHash { def tpe = NodeHash.ContractStorageMptNode }
  final case class StorageRootHash(bytes: Array[Byte]) extends NodeHash { def tpe = NodeHash.StorageRoot }
  final case class EvmcodeHash(bytes: Array[Byte]) extends NodeHash { def tpe = NodeHash.Evmcode }

  sealed trait RequestToPeer[M <: Message, R <: PeerResponse] extends Command {
    def id = peerId
    def peerId: String
    def messageToSend: MessageSerializable

    def processResponse(message: M): Option[R]
  }

  sealed trait PeerResponse extends Command {
    def id = peerId
    def peerId: String
  }

  final case class NodeDataResponse(peerId: String, receviceNode: ByteString) extends PeerResponse
  final case class NodeDataRequest(peerId: String, message: PV63.GetNodeData, requestHash: Hash) extends RequestToPeer[PV63.NodeData, NodeDataResponse] {
    def messageToSend = message

    def processResponse(nodeData: PV63.NodeData) = {
      if (nodeData.values.isEmpty) {
        None
      } else {
        nodeData.values find { value =>
          Hash(crypto.kec256(value.toArray)) == requestHash
        } map {
          NodeDataResponse(peerId, _)
        }
      }
    }
  }

  /**
   * https://blog.ethereum.org/2015/06/26/state-tree-pruning/
   */
  final case class NodeDatasResponse(peerId: String, nDownloadedNodes: Int, remainingHashes: List[NodeHash], childrenHashes: List[NodeHash], receviceAccounts: List[(Hash, ByteString)], receivedStorages: List[(Hash, ByteString)], receivdeEvmcodes: List[(Hash, ByteString)]) extends PeerResponse
  final case class NodeDatasRequest(peerId: String, message: PV63.GetNodeData, requestNodeHashes: List[NodeHash]) extends RequestToPeer[PV63.NodeData, NodeDatasResponse] {
    def messageToSend = message

    def processResponse(nodeData: PV63.NodeData) = {
      if (nodeData.values.isEmpty) {
        None
      } else {
        val requestHashes = requestNodeHashes.map(x => x.toHash -> x).toMap

        val (receivedNodeHashes, childrenHashes, receivedAccounts, receivedStorages, receivedEvmcodes) =
          nodeData.values.foldLeft((Set[NodeHash](), List[NodeHash](), List[(Hash, ByteString)](), List[(Hash, ByteString)](), List[(Hash, ByteString)]())) {
            case ((receivedHashes, childHashes, receivedAccounts, receivedStorages, receivedEvmcodes), value) =>
              val receivedHash = Hash(crypto.kec256(value.toArray))
              requestHashes.get(receivedHash) match {
                case None =>
                  (receivedHashes, childHashes, receivedAccounts, receivedStorages, receivedEvmcodes)

                case Some(x: StateMptNodeHash) =>
                  val node = nodeData.toMptNode(value)
                  val hashes = getStateNodeChildren(node)
                  (receivedHashes + x, childHashes ::: hashes, (x.toHash, value) :: receivedAccounts, receivedStorages, receivedEvmcodes)

                case Some(x: StorageRootHash) =>
                  val node = nodeData.toMptNode(value)
                  val hashes = getContractMptNodeChildren(node)
                  (receivedHashes + x, childHashes ::: hashes, receivedAccounts, (x.toHash, value) :: receivedStorages, receivedEvmcodes)

                case Some(x: ContractStorageMptNodeHash) =>
                  val node = nodeData.toMptNode(value)
                  val hashes = getContractMptNodeChildren(node)
                  (receivedHashes + x, childHashes ::: hashes, receivedAccounts, (x.toHash, value) :: receivedStorages, receivedEvmcodes)

                case Some(x: EvmcodeHash) =>
                  (receivedHashes + x, childHashes, receivedAccounts, receivedStorages, (x.toHash, value) :: receivedEvmcodes)
              }
          }

        val remainingHashes = requestNodeHashes filterNot receivedNodeHashes.contains

        Some(NodeDatasResponse(peerId, receivedNodeHashes.size, remainingHashes, childrenHashes, receivedAccounts, receivedStorages, receivedEvmcodes))
      }
    }

    private def getStateNodeChildren(mptNode: PV63.MptNode): List[NodeHash] = {
      mptNode match {
        case node @ PV63.MptLeaf(keyNibbles, value) =>
          val account = node.getAccount
          val codeHash = account.codeHash match {
            case Account.EmptyCodeHash => Nil
            case hash                  => List(EvmcodeHash(hash.bytes))
          }
          val storageHash = account.stateRoot match {
            case Account.EmptyStorageRootHash => Nil
            case hash                         => List(StorageRootHash(hash.bytes))
          }

          codeHash ::: storageHash

        case PV63.MptBranch(children, value) =>
          children.toList.collect {
            case Left(PV63.MptHash(child)) if child.nonEmpty => child
          }.map(hash => StateMptNodeHash(hash.bytes))

        case PV63.MptExtension(keyNibbles, child) =>
          child.fold(
            mptHash => List(StateMptNodeHash(mptHash.hash.bytes)),
            mptNode => Nil
          )
      }
    }

    private def getContractMptNodeChildren(mptNode: PV63.MptNode): List[NodeHash] = {
      mptNode match {
        case _: PV63.MptLeaf =>
          Nil

        case PV63.MptBranch(children, value) =>
          children.toList.collect {
            case Left(PV63.MptHash(child)) if child.nonEmpty => child
          }.map(hash => ContractStorageMptNodeHash(hash.bytes))

        case PV63.MptExtension(keyNibbles, child) =>
          child.fold(
            mptHash => List(ContractStorageMptNodeHash(mptHash.hash.bytes)),
            mptNode => Nil
          )
      }
    }
  }

  final case class ReceiptsResponse(peerId: String, remainingHashes: List[Hash], receivedHashes: List[Hash], receipts: List[Seq[Receipt]]) extends PeerResponse
  final case class ReceiptsRequest(peerId: String, message: PV63.GetReceipts) extends RequestToPeer[PV63.Receipts, ReceiptsResponse] {
    def messageToSend = message

    def processResponse(receipts: PV63.Receipts) = {
      val requestHashes = message.blockHashes.toList
      if (receipts.receiptsForBlocks.isEmpty) {
        None
      } else {
        val receivedReceipts = receipts.receiptsForBlocks.toList
        val receivedHashes = requestHashes.take(receivedReceipts.size)
        val remainingHashes = requestHashes.drop(receivedReceipts.size)

        Some(ReceiptsResponse(peerId, remainingHashes, receivedHashes, receivedReceipts))
      }
    }
  }

  final case class BlockBodiesResponse(peerId: String, remainingHashes: List[Hash], receivedHashes: List[Hash], bodies: List[PV62.BlockBody]) extends PeerResponse
  final case class BlockBodiesRequest(peerId: String, message: PV62.GetBlockBodies) extends RequestToPeer[PV62.BlockBodies, BlockBodiesResponse] {
    def messageToSend = message

    def processResponse(blockBodies: PV62.BlockBodies) = {
      val requestedHashes = message.hashes.toList
      if (blockBodies.bodies.isEmpty) {
        None
      } else {
        val receivedBodies = blockBodies.bodies.toList
        val receivedHashes = requestedHashes.take(receivedBodies.size)
        val remainingHashes = requestedHashes.drop(receivedBodies.size)

        Some(BlockBodiesResponse(peerId, remainingHashes, receivedHashes, receivedBodies))
      }
    }
  }

  final case class BlockHeadersResponse(peerId: String, headers: List[BlockHeader], isConsistent: Boolean) extends PeerResponse
  final case class BlockHeadersRequest(peerId: String, parentHeader: Option[BlockHeader], message: PV62.GetBlockHeaders) extends RequestToPeer[PV62.BlockHeaders, BlockHeadersResponse] {
    def messageToSend = message

    def processResponse(blockHeaders: PV62.BlockHeaders) = {
      val headers = (if (message.reverse) blockHeaders.headers.reverse else blockHeaders.headers).toList

      if (headers.nonEmpty) {
        if (parentHeader.fold(true) { prevHeader => headers.head.parentHash == prevHeader.hash } && isHeadersConsistent(headers)) {
          Some(BlockHeadersResponse(peerId, headers, true))
        } else {
          Some(BlockHeadersResponse(peerId, List(), false))
        }
      } else {
        None
      }
    }

    private def isHeadersConsistent(headers: List[BlockHeader]): Boolean = {
      if (headers.length > 1) {
        headers.zip(headers.tail).forall {
          case (parent, child) => parent.hash == child.parentHash && parent.number + 1 == child.number
        }
      } else {
        true
      }
    }
  }

}

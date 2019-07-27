package khipu.network.p2p.messages

import akka.util.ByteString
import khipu.Hash
import khipu.crypto
import khipu.domain.{ Account, Address, Receipt, TxLogEntry }
import khipu.network.p2p.{ Message, MessageSerializableImplicit }
import khipu.rlp
import khipu.rlp.RLPException
import khipu.rlp.RLPEncodeable
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.rlp.RLPSerializable
import khipu.rlp.RLPValue
import khipu.trie.HexPrefix
import scala.language.implicitConversions

object PV63 {

  object GetNodeData {
    val code: Int = Versions.SubProtocolOffset + 0x0d

    implicit final class GetNodeDataEnc(val underlying: GetNodeData) extends MessageSerializableImplicit[GetNodeData](underlying) with RLPSerializable {
      override def code: Int = GetNodeData.code
      override def toRLPEncodable: RLPEncodeable = toRlpList(msg.mptElementsHashes)
    }

    implicit final class GetNodeDataDec(val bytes: Array[Byte]) {
      def toGetNodeData: GetNodeData = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => GetNodeData(fromRlpList[Hash](rlpList))
        case _                => throw new RuntimeException("Cannot decode GetNodeData")
      }
    }
  }
  final case class GetNodeData(mptElementsHashes: Seq[Hash]) extends Message {
    override def code: Int = GetNodeData.code
    override def toString: String = {
      s"""GetNodeData{
         |hashes: ${mptElementsHashes.map(_.hexString)}
         |}
       """.stripMargin
    }
  }

  object AccountImplicits {
    implicit final class AccountEnc(val account: Account) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = {
        import account._
        RLPList(rlp.toRLPEncodable(nonce), rlp.toRLPEncodable(balance), stateRoot, codeHash)
      }
    }

    implicit final class AccountDec(val bytes: Array[Byte]) {
      def toAccount: Account = rlp.rawDecode(bytes) match {
        case RLPList(nonce, balance, stateRoot, codeHash) =>
          Account(rlp.toDataWord(nonce), rlp.toDataWord(balance), stateRoot, codeHash)
        case _ => throw new RuntimeException("Cannot decode Account")
      }
    }
  }

  final case class MptHash(hash: Hash)

  object MptNode {
    val BranchNodeChildLength = 16
    val BranchNodeIndexOfValue = 16
    val ExtensionNodeLength = 2
    val LeafNodeLength = 2
    val MaxNodeValueSize = 31
    val HashLength = 32

    implicit final class MptNodeEnc(obj: MptNode) extends RLPSerializable {
      def toRLPEncodable: RLPEncodeable = obj match {
        case MptLeaf(keyNibbles, value) =>
          RLPList(
            RLPValue(HexPrefix.encode(keyNibbles.toArray[Byte], isLeaf = true)),
            value
          )
        case MptExtension(keyNibbles, child) =>
          RLPList(
            RLPValue(HexPrefix.encode(keyNibbles.toArray[Byte], isLeaf = false)),
            child.fold(hash => hash.hash, node => node.toRLPEncodable)
          )
        case MptBranch(children, value) =>
          RLPList(children.map { e =>
            e.fold(mptHash => toEncodeable(mptHash.hash), node => node.toRLPEncodable)
          } :+ (value: RLPEncodeable): _*)
      }
    }

    implicit final class MptNodeDec(val bytes: Array[Byte]) {
      def toMptNode: MptNode = MptNodeRLPEncodableDec(rlp.rawDecode(bytes)).toMptNode
    }

    implicit final class MptNodeRLPEncodableDec(val rlpEncodable: RLPEncodeable) {
      def toMptNode: MptNode = rlpEncodable match {
        case rlpList: RLPList if rlpList.items.length == BranchNodeChildLength + 1 =>
          MptBranch(rlpList.items.take(BranchNodeChildLength).map(decodeChild), byteStringEncDec.decode(rlpList.items(BranchNodeIndexOfValue)))
        case RLPList(hpEncoded, value) =>
          HexPrefix.decode(hpEncoded: Array[Byte]) match {
            case (decoded, true) =>
              MptLeaf(ByteString(decoded), value)
            case (decoded, false) =>
              MptExtension(ByteString(decoded), decodeChild(value))
          }
        case _ =>
          throw new RuntimeException("Cannot decode NodeData")
      }

      private def decodeChild(rlpEncodable: RLPEncodeable): Either[MptHash, MptNode] = {
        val encodedLength = rlp.encode(rlpEncodable).length

        rlpEncodable match {
          case value: RLPValue if value.bytes.length == HashLength || value.bytes.length == 0 =>
            Left(MptHash(value))

          case list: RLPList if (list.items.length == ExtensionNodeLength || list.items.length == LeafNodeLength) && encodedLength <= MaxNodeValueSize =>
            Right(list.toMptNode)

          case list: RLPList if list.items.length == BranchNodeChildLength + 1 && encodedLength <= MaxNodeValueSize =>
            Right(list.toMptNode)

          case _ => throw new RuntimeException("unexpected value in node")
        }
      }
    }
  }

  object NodeData {
    val code: Int = Versions.SubProtocolOffset + 0x0e

    implicit final class NodeDataEnc(val underlying: NodeData) extends MessageSerializableImplicit[NodeData](underlying) with RLPSerializable {

      import khipu.network.p2p.messages.PV63.MptNode._

      override def code: Int = NodeData.code
      override def toRLPEncodable: RLPEncodeable = msg.values

      @throws[RLPException] //def getMptNode(index: Int): MptNode = msg.values(index).toArray[Byte].toMptNode
      def toMptNode(value: ByteString) = value.toArray[Byte].toMptNode
    }

    implicit final class NodeDataDec(val bytes: Array[Byte]) {
      def toNodeData: NodeData = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => NodeData(rlpList.items.map { e => e: ByteString })
        case _                => throw new RuntimeException("Cannot decode NodeData")
      }
    }
  }
  final case class NodeData(values: Seq[ByteString]) extends Message {
    override def code: Int = NodeData.code
    override def toString: String = {
      s"""NodeData{
         |values: ${values.map(b => khipu.toHexString(b))}
         |}
       """.stripMargin
    }
  }

  /**
   * https://github.com/ethereum/wiki/wiki/Patricia-Tree
   * A node in a Merkle Patricia trie is one of the following:
   * - NULL (represented as the empty string)
   * - branch A 17-item node [ v0 ... v15, vt ]
   * - leaf A 2-item node [ encodedPath, value ]
   * - extension A 2-item node [ encodedPath, key ]
   */
  sealed trait MptNode {
    lazy val hash: Hash = Hash(crypto.kec256(this.toBytes: Array[Byte]))
  }

  final case class MptBranch(children: Seq[Either[MptHash, MptNode]], value: ByteString) extends MptNode {
    require(children.length == 16, "MptBranch childHashes length have to be 16")

    override def toString: String = {
      val childrenString = children.map { e =>
        e.fold(
          { hash => s"Hash(${hash.hash.hexString})" },
          { node => s"Value(${node.toString})" }
        )
      }.mkString("(", ",\n", ")")

      s"""MptBranch{
         |children: $childrenString
         |value: ${khipu.toHexString(value)}
         |}
       """.stripMargin
    }
  }

  final case class MptExtension(keyNibbles: ByteString, child: Either[MptHash, MptNode]) extends MptNode {
    override def toString: String = {
      s"""MptExtension{
         |key nibbles: $keyNibbles
         |key nibbles length: ${keyNibbles.length}
         |key: ${khipu.toHexString(keyNibbles)}
         |childHash: s"Hash(${child.fold(_.hash.hexString, _.toString)})"
         |}
       """.stripMargin
    }
  }

  final case class MptLeaf(keyNibbles: ByteString, value: ByteString) extends MptNode {
    import AccountImplicits._

    def getAccount: Account = value.toArray[Byte].toAccount

    override def toString: String = {
      s"""MptLeaf{
         |key nibbles: $keyNibbles
         |key nibbles length: ${keyNibbles.length}
         |key: ${khipu.toHexString(keyNibbles)}
         |value: ${khipu.toHexString(value)}
         |}
       """.stripMargin
    }
  }

  object GetReceipts {
    val code: Int = Versions.SubProtocolOffset + 0x0f

    implicit final class GetReceiptsEnc(val underlying: GetReceipts) extends MessageSerializableImplicit[GetReceipts](underlying) with RLPSerializable {
      override def code: Int = GetReceipts.code

      override def toRLPEncodable: RLPEncodeable = msg.blockHashes: RLPList
    }

    implicit final class GetReceiptsDec(val bytes: Array[Byte]) {
      def toGetReceipts: GetReceipts = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => GetReceipts(fromRlpList[Hash](rlpList))
        case _                => throw new RuntimeException("Cannot decode GetReceipts")
      }
    }
  }
  final case class GetReceipts(blockHashes: Seq[Hash]) extends Message {
    override def code: Int = GetReceipts.code
    override def toString: String = {
      s"""GetReceipts{
         |blockHashes: ${blockHashes.map(_.hexString)}
         |}
       """.stripMargin
    }
  }

  object TxLogEntryImplicits {

    implicit final class TxLogEntryEnc(logEntry: TxLogEntry) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = {
        import logEntry._
        RLPList(loggerAddress.bytes, logTopics, data)
      }
    }

    implicit final class TxLogEntryDec(rlpEncodeable: RLPEncodeable) {
      def toTxLogEntry: TxLogEntry = rlpEncodeable match {
        case RLPList(loggerAddress, logTopics: RLPList, data) =>
          TxLogEntry(Address(loggerAddress: ByteString), fromRlpList[ByteString](logTopics).toList, data)

        case _ => throw new RuntimeException("Cannot decode TransactionLog")
      }
    }
  }

  object ReceiptImplicits {
    import TxLogEntryImplicits._

    implicit final class ReceiptEnc(msg: Receipt) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = {
        import msg._
        RLPList(postTxState, cumulativeGasUsed, logsBloomFilter, RLPList(logs.map(_.toRLPEncodable): _*))
      }
    }

    implicit final class ReceiptSeqEnc(receipts: Seq[Receipt]) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = RLPList(receipts.map(_.toRLPEncodable): _*)
    }

    implicit final class ReceiptDec(val bytes: Array[Byte]) {
      def toReceipt: Receipt = ReceiptRLPEncodableDec(rlp.rawDecode(bytes)).toReceipt

      def toReceipts: Seq[Receipt] = rlp.rawDecode(bytes) match {
        case RLPList(items @ _*) => items.map(_.toReceipt)
        case _                   => throw new RuntimeException("Cannot decode Receipts")
      }
    }

    implicit final class ReceiptRLPEncodableDec(val rlpEncodeable: RLPEncodeable) {
      def toReceipt: Receipt = rlpEncodeable match {
        case RLPList(postTxState, cumulativeGasUsed, logsBloomFilter, logs: RLPList) =>
          Receipt(postTxState, cumulativeGasUsed, logsBloomFilter, logs.items.map(_.toTxLogEntry))
        case _ =>
          throw new RuntimeException("Cannot decode Receipt")
      }
    }
  }

  object Receipts {
    val code: Int = Versions.SubProtocolOffset + 0x10

    implicit final class ReceiptsEnc(val underlying: Receipts) extends MessageSerializableImplicit[Receipts](underlying) with RLPSerializable {
      import ReceiptImplicits._

      override def code: Int = Receipts.code
      override def toRLPEncodable: RLPEncodeable = RLPList(
        msg.receiptsForBlocks.map { rs =>
          RLPList(rs.map(_.toRLPEncodable): _*)
        }: _*
      )
    }

    implicit final class ReceiptsDec(val bytes: Array[Byte]) {
      import ReceiptImplicits._

      def toReceipts: Receipts = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => Receipts(rlpList.items.collect { case r: RLPList => r.items.map(_.toReceipt) })
        case _                => throw new RuntimeException("Cannot decode Receipts")
      }
    }
  }
  final case class Receipts(receiptsForBlocks: Seq[Seq[Receipt]]) extends Message {
    override def code: Int = Receipts.code
  }
}

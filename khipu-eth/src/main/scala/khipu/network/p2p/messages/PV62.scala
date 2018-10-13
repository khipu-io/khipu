package khipu.network.p2p.messages

import akka.util.ByteString
import khipu.domain.{ BlockHeader, SignedTransaction }
import khipu.network.p2p.messages.CommonMessages.SignedTransactions
import khipu.network.p2p.{ Message, MessageSerializableImplicit }
import khipu.Hash
import khipu.rlp
import khipu.rlp.RLPEncodeable
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.rlp.RLPSerializable
import khipu.rlp.RLPValue

object PV62 {
  object BlockHash {
    implicit final class BlockHashEnc(blockHash: BlockHash) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = RLPList(blockHash.hash, blockHash.number)
    }

    implicit final class BlockHashDec(val bytes: Array[Byte]) {
      def toBlockHash: BlockHash = BlockHashRLPEncodableDec(bytes).toBlockHash
    }

    implicit final class BlockHashRLPEncodableDec(val rlpEncodeable: RLPEncodeable) {
      def toBlockHash: BlockHash = rlpEncodeable match {
        case RLPList(hash, number) => BlockHash(hash, number)
        case _                     => throw new RuntimeException("Cannot decode BlockHash")
      }
    }
  }
  final case class BlockHash(hash: ByteString, number: Long) {
    override def toString: String = {
      s"""BlockHash {
         |hash: ${khipu.toHexString(hash)}
         |number: $number
         |}""".stripMargin
    }
  }

  object NewBlockHashes {
    val code: Int = Versions.SubProtocolOffset + 0x01

    implicit final class NewBlockHashesEnc(val underlying: NewBlockHashes) extends MessageSerializableImplicit[NewBlockHashes](underlying) with RLPSerializable {
      import BlockHash._

      override def code: Int = NewBlockHashes.code
      override def toRLPEncodable: RLPEncodeable = RLPList(msg.hashes.map(_.toRLPEncodable): _*)
    }

    implicit final class NewBlockHashesDec(val bytes: Array[Byte]) {
      import BlockHash._
      def toNewBlockHashes: NewBlockHashes = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => NewBlockHashes(rlpList.items.map(_.toBlockHash))
        case _                => throw new RuntimeException("Cannot decode NewBlockHashes")
      }
    }
  }
  final case class NewBlockHashes(hashes: Seq[BlockHash]) extends Message {
    override def code: Int = NewBlockHashes.code
  }

  object GetBlockHeaders {
    val code: Int = Versions.SubProtocolOffset + 0x03

    implicit final class GetBlockHeadersEnc(val underlying: GetBlockHeaders)
        extends MessageSerializableImplicit[GetBlockHeaders](underlying) with RLPSerializable {

      override def code: Int = GetBlockHeaders.code

      override def toRLPEncodable: RLPEncodeable = {
        import msg._
        block match {
          case Left(blockNumber) => RLPList(blockNumber, maxHeaders, skip, if (reverse) 1 else 0)
          case Right(blockHash)  => RLPList(blockHash, maxHeaders, skip, if (reverse) 1 else 0)
        }
      }
    }

    implicit final class GetBlockHeadersDec(val bytes: Array[Byte]) {
      def toGetBlockHeaders: GetBlockHeaders = rlp.rawDecode(bytes) match {
        case RLPList((block: RLPValue), maxHeaders, skip, reverse) if block.bytes.length < 32 =>
          GetBlockHeaders(Left(block), maxHeaders, skip, (reverse: Int) == 1)

        case RLPList((block: RLPValue), maxHeaders, skip, reverse) =>
          GetBlockHeaders(Right(block), maxHeaders, skip, (reverse: Int) == 1)

        case _ => throw new RuntimeException("Cannot decode GetBlockHeaders")
      }
    }
  }
  final case class GetBlockHeaders(block: Either[Long, Hash], maxHeaders: Long, skip: Long, reverse: Boolean) extends Message {
    override def code: Int = GetBlockHeaders.code

    override def toString: String = {
      s"""GetBlockHeaders{
         |block: ${block.fold(a => a, _.hexString)}
         |maxHeaders: $maxHeaders
         |skip: $skip
         |reverse: $reverse
         |}
     """.stripMargin
    }
  }

  object BlockHeaderImplicits {
    def toBlockHeader(rlpEncodeable: RLPEncodeable): BlockHeader =
      rlpEncodeable match {
        case RLPList(parentHash, ommersHash, beneficiary, stateRoot, transactionsRoot, receiptsRoot,
          logsBloom, difficulty, number, gasLimit, gasUsed, unixTimestamp, extraData, mixHash, nonce) =>
          BlockHeader(parentHash, ommersHash, beneficiary, stateRoot, transactionsRoot, receiptsRoot,
            logsBloom, rlp.toUInt256(difficulty), number, gasLimit, gasUsed, unixTimestamp, extraData, mixHash, nonce)
      }

    implicit final class BlockHeaderEnc(blockHeader: BlockHeader) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = {
        import blockHeader._
        RLPList(parentHash, ommersHash, beneficiary, stateRoot, transactionsRoot, receiptsRoot,
          logsBloom, rlp.toRLPEncodable(difficulty), number, gasLimit, gasUsed, unixTimestamp, extraData, mixHash, nonce)
      }
    }

    implicit final class BlockHeaderSeqEnc(blockHeaders: Seq[BlockHeader]) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = RLPList(blockHeaders.map(_.toRLPEncodable): _*)
    }

    implicit final class BlockheaderDec(val bytes: Array[Byte]) {
      def toBlockHeader: BlockHeader = BlockheaderEncodableDec(rlp.rawDecode(bytes)).toBlockHeader
    }

    implicit final class BlockheaderEncodableDec(val rlpEncodeable: RLPEncodeable) {
      def toBlockHeader: BlockHeader = BlockHeaderImplicits.toBlockHeader(rlpEncodeable)
    }
  }

  object BlockBody {
    import khipu.network.p2p.messages.CommonMessages.SignedTransactions._
    import BlockHeaderImplicits._

    def toBlockBody(rlpEncodeable: RLPEncodeable): BlockBody = rlpEncodeable match {
      case RLPList(transactions: RLPList, uncles: RLPList) =>
        BlockBody(
          transactions.items map SignedTransactions.toSignedTransaction,
          uncles.items map BlockHeaderImplicits.toBlockHeader
        )
      case _ => throw new RuntimeException("Cannot decode BlockBody")
    }

    implicit final class BlockBodyEnc(msg: BlockBody) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = RLPList(
        RLPList(msg.transactionList.map(_.toRLPEncodable): _*),
        RLPList(msg.uncleNodesList.map(_.toRLPEncodable): _*)
      )
    }

    implicit final class BlockBodyDec(val bytes: Array[Byte]) {
      def toBlockBody: BlockBody = BlockBody.toBlockBody(rlp.rawDecode(bytes))
    }

    implicit final class BlockBodyRLPEncodableDec(val rlpEncodeable: RLPEncodeable) {
      def toBlockBody: BlockBody = BlockBody.toBlockBody(rlpEncodeable)
    }
  }
  final case class BlockBody(transactionList: Seq[SignedTransaction], uncleNodesList: Seq[BlockHeader]) {
    override def toString: String =
      s"""BlockBody{
         |transactionList: $transactionList
         |uncleNodesList: $uncleNodesList
         |}
    """.stripMargin
  }

  object BlockBodies {
    val code: Int = Versions.SubProtocolOffset + 0x06

    implicit final class BlockBodiesEnc(val underlying: BlockBodies) extends MessageSerializableImplicit[BlockBodies](underlying) with RLPSerializable {
      override def code: Int = BlockBodies.code

      override def toRLPEncodable: RLPEncodeable = RLPList(msg.bodies.map(_.toRLPEncodable): _*)
    }

    implicit final class BlockBodiesDec(val bytes: Array[Byte]) {
      import BlockBody._
      def toBlockBodies: BlockBodies = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => BlockBodies(rlpList.items map BlockBody.toBlockBody)
        case _                => throw new RuntimeException("Cannot decode BlockBodies")
      }
    }
  }
  final case class BlockBodies(bodies: Seq[BlockBody]) extends Message {
    val code: Int = BlockBodies.code
  }

  object BlockHeaders {
    val code: Int = Versions.SubProtocolOffset + 0x04

    implicit final class BlockHeadersEnc(val underlying: BlockHeaders) extends MessageSerializableImplicit[BlockHeaders](underlying) with RLPSerializable {
      import BlockHeaderImplicits._

      override def code: Int = BlockHeaders.code
      override def toRLPEncodable: RLPEncodeable = RLPList(msg.headers.map(_.toRLPEncodable): _*)
    }

    implicit final class BlockHeadersDec(val bytes: Array[Byte]) {
      import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._

      def toBlockHeaders: BlockHeaders = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => BlockHeaders(rlpList.items.map(_.toBlockHeader))
        case _                => throw new RuntimeException("Cannot decode BlockHeaders")
      }
    }
  }
  final case class BlockHeaders(headers: Seq[BlockHeader]) extends Message {
    override def code: Int = BlockHeaders.code
  }

  object GetBlockBodies {
    val code: Int = Versions.SubProtocolOffset + 0x05

    implicit final class GetBlockBodiesEnc(val underlying: GetBlockBodies)
        extends MessageSerializableImplicit[GetBlockBodies](underlying) with RLPSerializable {

      override def code: Int = GetBlockBodies.code
      override def toRLPEncodable: RLPEncodeable = toRlpList(msg.hashes)
    }

    implicit final class GetBlockBodiesDec(val bytes: Array[Byte]) {
      def toGetBlockBodies: GetBlockBodies = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => GetBlockBodies(fromRlpList[Hash](rlpList))

        case _                => throw new RuntimeException("Cannot decode BlockHeaders")
      }
    }
  }
  final case class GetBlockBodies(hashes: Seq[Hash]) extends Message {
    override def code: Int = GetBlockBodies.code
    override def toString: String = {
      s"""GetBlockBodies {
         |hashes: ${hashes.map(_.hexString)}
         |}
     """.stripMargin
    }
  }
}

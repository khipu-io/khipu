package khipu.network.p2p.messages

import khipu.Hash
import khipu.crypto
import khipu.crypto.ECDSASignature
import khipu.domain.Address
import khipu.domain.Block
import khipu.domain.SignedTransaction
import khipu.domain.Transaction
import khipu.network.p2p.Message
import khipu.network.p2p.MessageSerializableImplicit
import khipu.rlp
import khipu.rlp.RLPEncodeable
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.rlp.RLPSerializable
import khipu.rlp.RLPValue
import khipu.vm.UInt256

object CommonMessages {
  object Status {
    val code: Int = Versions.SubProtocolOffset + 0x00

    implicit final class StatusEnc(val underlying: Status) extends MessageSerializableImplicit[Status](underlying) with RLPSerializable {
      override def code: Int = Status.code
      override def toRLPEncodable: RLPEncodeable = {
        import msg._
        RLPList(protocolVersion, networkId, rlp.toRLPEncodable(totalDifficulty), bestHash, genesisHash)
      }
    }

    implicit final class StatusDec(val bytes: Array[Byte]) {
      def toStatus: Status = rlp.rawDecode(bytes) match {
        case RLPList(protocolVersion, networkId, totalDifficulty, bestHash, genesisHash) =>
          Status(protocolVersion, networkId, rlp.toUInt256(totalDifficulty), bestHash, genesisHash)
        case _ => throw new RuntimeException("Cannot decode Status")
      }
    }
  }
  final case class Status(protocolVersion: Int, networkId: Int, totalDifficulty: UInt256, bestHash: Hash, genesisHash: Hash) extends Message {
    override def code: Int = Status.code
    override def toString: String = {
      s"""Status {
         |protocolVersion: $protocolVersion
         |networkId: $networkId
         |totalDifficulty: $totalDifficulty
         |bestHash: ${bestHash.hexString}
         |genesisHash: ${genesisHash.hexString}
         |}""".stripMargin
    }
  }

  object SignedTransactions {
    val code: Int = Versions.SubProtocolOffset + 0x02

    implicit final class SignedTransactionEnc(val signedTx: SignedTransaction) extends RLPSerializable {
      override def toRLPEncodable: RLPEncodeable = {
        import signedTx._
        import signedTx.tx._
        RLPList(
          rlp.toRLPEncodable(nonce),
          rlp.toRLPEncodable(gasPrice),
          gasLimit,
          receivingAddress.map(_.toArray).getOrElse(Array.emptyByteArray): Array[Byte],
          rlp.toRLPEncodable(value),
          payload,
          ECDSASignature.getEncodeV(signature.v, chainId).fold(x => x, y => y),
          signature.r,
          signature.s
        )
      }
    }

    implicit final class SignedTransactionsEnc(val underlying: SignedTransactions)
        extends MessageSerializableImplicit[SignedTransactions](underlying) with RLPSerializable {

      override def code: Int = SignedTransactions.code
      override def toRLPEncodable: RLPEncodeable = RLPList(msg.txs.map(_.toRLPEncodable): _*)
    }

    def toSignedTransaction(rlpEncodeable: RLPEncodeable): SignedTransaction = rlpEncodeable match {
      case RLPList(nonce, gasPrice, gasLimit, RLPValue(receivingAddress), value, payload, v, r, s) =>
        val receivingAddressOpt = if (receivingAddress.length == 0) None else Some(Address(receivingAddress))
        val realV = ECDSASignature.getRealV(v)
        val chainId = ECDSASignature.extractChainIdFromV(v)

        SignedTransaction(
          Transaction(rlp.toUInt256(nonce), rlp.toUInt256(gasPrice), gasLimit, receivingAddressOpt, rlp.toUInt256(value), payload),
          r,
          s,
          realV,
          chainId
        )
    }

    implicit final class SignedTransactionsDec(val bytes: Array[Byte]) {
      def toSignedTransactions: SignedTransactions = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => SignedTransactions(rlpList.items map toSignedTransaction)
        case _                => throw new RuntimeException("Cannot decode SignedTransactions")
      }
    }

    implicit final class SignedTransactionRlpEncodableDec(val rlpEncodeable: RLPEncodeable) {
      def toSignedTransaction: SignedTransaction = SignedTransactions.toSignedTransaction(rlpEncodeable)
    }

    implicit final class SignedTransactionDec(val bytes: Array[Byte]) {
      def toSignedTransaction: SignedTransaction = rlp.rawDecode(bytes).toSignedTransaction
    }
  }
  final case class SignedTransactions(txs: Seq[SignedTransaction]) extends Message {
    override def code: Int = SignedTransactions.code
  }

  object NewBlock {
    val code: Int = Versions.SubProtocolOffset + 0x07

    implicit final class NewBlockEnc(val underlying: NewBlock) extends MessageSerializableImplicit[NewBlock](underlying) with RLPSerializable {
      import khipu.network.p2p.messages.CommonMessages.SignedTransactions._
      import khipu.network.p2p.messages.PV62.BlockHeaderImplicits._

      override def code: Int = NewBlock.code
      override def toRLPEncodable: RLPEncodeable = {
        import msg._
        RLPList(
          RLPList(
            block.header.toRLPEncodable,
            RLPList(block.body.transactionList.map(_.toRLPEncodable): _*),
            RLPList(block.body.uncleNodesList.map(_.toRLPEncodable): _*)
          ),
          rlp.toRLPEncodable(totalDifficulty)
        )
      }
    }

    implicit final class NewBlockDec(val bytes: Array[Byte]) {
      import khipu.network.p2p.messages.PV62._
      import BlockHeaderImplicits._
      import SignedTransactions._

      def toNewBlock: NewBlock = rlp.rawDecode(bytes) match {
        case RLPList(RLPList(blockHeader, (transactionList: RLPList), (uncleNodesList: RLPList)), totalDifficulty) =>
          NewBlock(
            Block(
              blockHeader.toBlockHeader,
              BlockBody(
                transactionList.items.map(_.toSignedTransaction),
                uncleNodesList.items.map(_.toBlockHeader)
              )
            ),
            rlp.toUInt256(totalDifficulty)
          )
        case _ => throw new RuntimeException("Cannot decode NewBlock")
      }
    }
  }
  final case class NewBlock(block: Block, totalDifficulty: UInt256, txInParallel: Int = 0) extends Message {
    override def code: Int = NewBlock.code
    override def toString: String = {
      s"""NewBlock {
         |block: $block
         |totalDifficulty: $totalDifficulty
         |}""".stripMargin
    }
  }
}

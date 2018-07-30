package khipu.network.p2p.messages

import akka.util.ByteString
import java.math.BigInteger
import khipu.network.p2p.{ Message, MessageSerializableImplicit }
import khipu.rlp
import khipu.rlp.RLPEncodeable
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.rlp.RLPSerializable
import khipu.rlp.RLPValue

object PV61 {

  object NewBlockHashes {
    val code: Int = Versions.SubProtocolOffset + 0x01

    implicit final class NewBlockHashesEnc(val underlying: NewBlockHashes)
        extends MessageSerializableImplicit[NewBlockHashes](underlying) with RLPSerializable {

      override def code: Int = NewBlockHashes.code

      override def toRLPEncodable: RLPEncodeable = RLPList(msg.hashes.map(e => RLPValue(e.toArray[Byte])): _*)
    }

    implicit final class NewBlockHashesDec(val bytes: Array[Byte]) extends AnyVal {
      def toNewBlockHashes: NewBlockHashes = rlp.rawDecode(bytes) match {
        case rlpList: RLPList => NewBlockHashes(rlpList.items.map(e => ByteString(e: Array[Byte])))
        case _                => throw new RuntimeException("Cannot decode NewBlockHashes")
      }

    }
  }
  final case class NewBlockHashes(hashes: Seq[ByteString]) extends Message {
    override def code: Int = NewBlockHashes.code
  }

  object BlockHashesFromNumber {
    val code: Int = Versions.SubProtocolOffset + 0x08

    implicit final class BlockHashesFromNumberEnc(val underlying: BlockHashesFromNumber)
        extends MessageSerializableImplicit[BlockHashesFromNumber](underlying) with RLPSerializable {

      override def code: Int = BlockHashesFromNumber.code

      override def toRLPEncodable: RLPEncodeable = RLPList(msg.number, msg.maxBlocks)
    }

    implicit final class BlockHashesFromNumberDec(val bytes: Array[Byte]) extends AnyVal {
      def toBlockHashesFromNumber: BlockHashesFromNumber = rlp.rawDecode(bytes) match {
        case RLPList(number, maxBlocks) => BlockHashesFromNumber(number, maxBlocks)
        case _                          => throw new RuntimeException("Cannot decode BlockHashesFromNumber")
      }
    }
  }
  final case class BlockHashesFromNumber(number: BigInteger, maxBlocks: BigInteger) extends Message {
    override def code: Int = BlockHashesFromNumber.code
  }

}

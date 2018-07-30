package khipu.rlp

import akka.util.ByteString
import khipu.rlp.RLP._
import khipu.rlp.RLPImplicitConversions._
import khipu.vm.UInt256
import scala.language.implicitConversions

object UInt256RLPImplicits {

  implicit final class UInt256Enc(obj: UInt256) extends RLPSerializable {
    override def toRLPEncodable: RLPEncodeable =
      RLPValue(if (obj.equals(UInt256.Zero)) byteToByteArray(0: Byte) else obj.nonZeroLeadingBytes)
  }

  implicit final class UInt256Dec(val bytes: ByteString) extends AnyVal {
    def toUInt256: UInt256 = UInt256RLPEncodableDec(rawDecode(bytes.toArray)).toUInt256
  }

  implicit final class UInt256FromBytesDec(val bytes: Array[Byte]) extends AnyVal {
    def toUInt256: UInt256 = UInt256RLPEncodableDec(rawDecode(bytes)).toUInt256
  }

  implicit final class UInt256RLPEncodableDec(val rLPEncodeable: RLPEncodeable) extends AnyVal {
    def toUInt256: UInt256 = rLPEncodeable match {
      case RLPValue(b) => UInt256(b)
      case _           => throw RLPException("src is not an RLPValue")
    }
  }

}

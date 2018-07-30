package khipu.network.rlpx.auth

import akka.util.ByteString
import khipu.crypto
import khipu.crypto.ECDSASignature
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.{ RLPDecoder, RLPEncodeable, RLPEncoder, RLPList }
import org.spongycastle.math.ec.ECPoint

object AuthResponseMessageV4 {

  implicit val rlpEncDec = new RLPEncoder[AuthResponseMessageV4] with RLPDecoder[AuthResponseMessageV4] {
    override def encode(obj: AuthResponseMessageV4): RLPEncodeable = {
      import obj._
      //byte 0 of encoded ECC point indicates that it is uncompressed point, it is part of spongycastle encoding
      RLPList(ephemeralPublicKey.getEncoded(false).drop(1), nonce.toArray[Byte], version)
    }

    override def decode(rlp: RLPEncodeable): AuthResponseMessageV4 = rlp match {
      case RLPList(ephemeralPublicKeyBytes, nonce, version, _*) =>
        val ephemeralPublicKey = crypto.curve.getCurve.decodePoint(ECDSASignature.UncompressedIndicator +: (ephemeralPublicKeyBytes: Array[Byte]))
        AuthResponseMessageV4(ephemeralPublicKey, ByteString(nonce: Array[Byte]), version)
      case _ => throw new RuntimeException("Cannot decode auth response message")
    }
  }
}
final case class AuthResponseMessageV4(ephemeralPublicKey: ECPoint, nonce: ByteString, version: Int)

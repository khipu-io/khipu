package khipu.network.rlpx.auth

import akka.util.ByteString
import khipu.crypto
import khipu.crypto.ECDSASignature
import khipu.rlp
import khipu.rlp.RLPEncodeable
import khipu.rlp.RLPImplicitConversions._
import khipu.rlp.RLPImplicits._
import khipu.rlp.RLPList
import khipu.rlp.RLPSerializable
import org.spongycastle.math.ec.ECPoint

object AuthInitiateMessageV4 extends AuthInitiateEcdsaCodec {

  implicit final class AuthInitiateMessageV4Enc(obj: AuthInitiateMessageV4) extends RLPSerializable {
    override def toRLPEncodable: RLPEncodeable = {
      import obj._
      //byte 0 of encoded ECC point indicates that it is uncompressed point, it is part of spongycastle encoding
      RLPList(encodeECDSA(signature), publicKey.getEncoded(false).drop(1), nonce, version)
    }
  }

  implicit final class AuthInitiateMessageV4Dec(val bytes: Array[Byte]) extends AnyVal {
    def toAuthInitiateMessageV4: AuthInitiateMessageV4 = rlp.rawDecode(bytes) match {
      case RLPList(signatureBytes, publicKeyBytes, nonce, version, _*) =>
        val signature = decodeECDSA(signatureBytes)
        val publicKey = crypto.curve.getCurve.decodePoint(ECDSASignature.UncompressedIndicator +: (publicKeyBytes: Array[Byte]))
        AuthInitiateMessageV4(signature, publicKey, ByteString(nonce: Array[Byte]), version)
      case _ => throw new RuntimeException("Cannot decode auth initiate message")
    }
  }
}
final case class AuthInitiateMessageV4(signature: ECDSASignature, publicKey: ECPoint, nonce: ByteString, version: Int)

package khipu.network.rlpx.auth

import akka.util.ByteString
import khipu.crypto
import khipu.crypto.ECDSASignature
import org.spongycastle.math.ec.ECPoint

object AuthInitiateMessage extends AuthInitiateEcdsaCodec {
  val NonceLength = 32
  val EphemeralHashLength = 32
  val PublicKeyLength = 64
  val KnownPeerLength = 1
  val EncodedLength: Int = ECDSASignature.EncodedLength + EphemeralHashLength + PublicKeyLength + NonceLength + KnownPeerLength

  def decode(input: Array[Byte]): AuthInitiateMessage = {
    val publicKeyIndex = ECDSASignature.EncodedLength + EphemeralHashLength
    val nonceIndex = publicKeyIndex + PublicKeyLength
    val knownPeerIndex = nonceIndex + NonceLength

    AuthInitiateMessage(
      signature = decodeECDSA(input.take(ECDSASignature.EncodedLength)),
      ephemeralPublicHash = ByteString(input.slice(ECDSASignature.EncodedLength, publicKeyIndex)),
      publicKey = crypto.curve.getCurve.decodePoint(ECDSASignature.UncompressedIndicator +: input.slice(publicKeyIndex, nonceIndex)),
      nonce = ByteString(input.slice(nonceIndex, knownPeerIndex)),
      knownPeer = input(knownPeerIndex) == 1
    )
  }
}
final case class AuthInitiateMessage(
    signature:           ECDSASignature,
    ephemeralPublicHash: ByteString,
    publicKey:           ECPoint,
    nonce:               ByteString,
    knownPeer:           Boolean
) extends AuthInitiateEcdsaCodec {

  lazy val encoded: ByteString = {
    encodeECDSA(signature) ++
      ephemeralPublicHash ++
      //byte 0 of encoded ECC point indicates that it is uncompressed point, it is part of spongycastle encoding
      publicKey.getEncoded(false).drop(1) ++
      nonce ++
      ByteString(if (knownPeer) 1.toByte else 0.toByte)
  }
}

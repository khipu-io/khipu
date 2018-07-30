package khipu.network.rlpx.auth

import akka.util.ByteString
import khipu.crypto
import khipu.crypto.ECDSASignature
import org.spongycastle.math.ec.ECPoint

object AuthResponseMessage {

  private val PublicKeyLength = 64
  private val NonceLength = 32
  private val KnownPeerLength = 1

  val EncodedLength: Int = PublicKeyLength + NonceLength + KnownPeerLength

  def decode(input: Array[Byte]): AuthResponseMessage = {
    AuthResponseMessage(
      ephemeralPublicKey = crypto.curve.getCurve.decodePoint(ECDSASignature.UncompressedIndicator +: input.take(PublicKeyLength)),
      nonce = ByteString(input.slice(PublicKeyLength, PublicKeyLength + NonceLength)),
      knownPeer = input(PublicKeyLength + NonceLength) == 1
    )
  }
}
final case class AuthResponseMessage(ephemeralPublicKey: ECPoint, nonce: ByteString, knownPeer: Boolean) {

  lazy val encoded: ByteString = ByteString(
    ephemeralPublicKey.getEncoded(false).drop(1) ++
      nonce ++
      Array(if (knownPeer) 1.toByte else 0.toByte)
  )
}

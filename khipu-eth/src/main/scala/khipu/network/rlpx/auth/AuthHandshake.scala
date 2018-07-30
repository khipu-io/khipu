package khipu.network.rlpx.auth

import akka.util.ByteString
import java.net.URI
import java.nio.ByteBuffer
import java.security.SecureRandom
import khipu.crypto
import khipu.crypto.ECIESCoder
import khipu.crypto.ECDSASignature
import khipu.network.rlpx.Secrets
import khipu.rlp
import khipu.util.BytesUtil._
import org.spongycastle.crypto.AsymmetricCipherKeyPair
import org.spongycastle.crypto.agreement.ECDHBasicAgreement
import org.spongycastle.crypto.digests.KeccakDigest
import org.spongycastle.crypto.params.{ ECPrivateKeyParameters, ECPublicKeyParameters }
import org.spongycastle.math.ec.ECPoint
import scala.util.Random

sealed trait AuthHandshakeResult
final case class AuthHandshakeSuccess(secrets: Secrets, remotePubKey: ByteString) extends AuthHandshakeResult
case object AuthHandshakeFailure extends AuthHandshakeResult

object AuthHandshake {
  val InitiatePacketLength = AuthInitiateMessage.EncodedLength + ECIESCoder.OverheadSize
  val ResponsePacketLength = AuthResponseMessage.EncodedLength + ECIESCoder.OverheadSize

  val NonceSize = 32
  val MacSize = 256
  val SecretSize = 32
  val MinPadding = 100
  val MaxPadding = 300

  val Version = 4

  def apply(nodeKey: AsymmetricCipherKeyPair, secureRandom: SecureRandom): AuthHandshake = {
    val nonce = crypto.secureRandomByteArray(secureRandom, NonceSize)
    new AuthHandshake(nodeKey, ByteString(nonce), crypto.generateKeyPair(secureRandom), secureRandom)
  }
}
final class AuthHandshake private (
    nodeKey:      AsymmetricCipherKeyPair,
    nonce:        ByteString,
    ephemeralKey: AsymmetricCipherKeyPair,
    secureRandom: SecureRandom
) {
  import AuthHandshake._

  private var isInitiator: Boolean = false
  private var initiatePacketOpt: Option[ByteString] = None
  private var responsePacketOpt: Option[ByteString] = None
  private var remotePubKeyOpt: Option[ECPoint] = None

  def initiate(uri: URI): ByteString = {
    val remotePubKey = ECDSASignature.publicKeyFromNodeId(uri.getUserInfo)
    val message = createAuthInitiateMessageV4(remotePubKey)
    val encoded: Array[Byte] = message.toBytes
    val padded = encoded ++ randomBytes(Random.nextInt(MaxPadding - MinPadding) + MinPadding)
    val encryptedSize = padded.length + ECIESCoder.OverheadSize
    val sizePrefix = ByteBuffer.allocate(2).putShort(encryptedSize.toShort).array
    val encryptedPayload = ECIESCoder.encrypt(remotePubKey, secureRandom, padded, Some(sizePrefix))
    val packet = ByteString(sizePrefix ++ encryptedPayload)

    this.isInitiator = true
    this.initiatePacketOpt = Some(packet)
    this.remotePubKeyOpt = Some(remotePubKey)

    packet
  }

  def handleResponseMessage(data: ByteString): AuthHandshakeResult = {
    val plaintext = ECIESCoder.decrypt(nodeKey.getPrivate.asInstanceOf[ECPrivateKeyParameters].getD, data.toArray)
    val message = AuthResponseMessage.decode(plaintext)

    this.responsePacketOpt = Some(data)
    finalizeHandshake(message.ephemeralPublicKey, message.nonce)
  }

  def handleResponseMessageV4(data: ByteString): AuthHandshakeResult = {
    val sizeBytes = data.take(2)
    val encryptedPayload = data.drop(2)

    val plaintext = ECIESCoder.decrypt(
      privKey = nodeKey.getPrivate.asInstanceOf[ECPrivateKeyParameters].getD,
      cipher = encryptedPayload.toArray,
      macData = Some(sizeBytes.toArray)
    )

    val message = rlp.decode[AuthResponseMessageV4](plaintext)

    this.responsePacketOpt = Some(data)
    finalizeHandshake(message.ephemeralPublicKey, message.nonce)
  }

  def handleInitialMessage(data: ByteString): (ByteString, AuthHandshakeResult) = {
    val plaintext = ECIESCoder.decrypt(nodeKey.getPrivate.asInstanceOf[ECPrivateKeyParameters].getD, data.toArray)
    val message = AuthInitiateMessage.decode(plaintext)

    val response = AuthResponseMessage(
      ephemeralPublicKey = ephemeralKey.getPublic.asInstanceOf[ECPublicKeyParameters].getQ,
      nonce = nonce,
      knownPeer = false
    )

    val encryptedPacket = ByteString(ECIESCoder.encrypt(message.publicKey, secureRandom, response.encoded.toArray, None))

    val remoteEphemeralKey = extractEphemeralKey(message.signature, message.nonce, message.publicKey)

    this.initiatePacketOpt = Some(data)
    this.responsePacketOpt = Some(encryptedPacket)
    this.remotePubKeyOpt = Some(message.publicKey)
    val handshakeResult = finalizeHandshake(remoteEphemeralKey, message.nonce)

    (encryptedPacket, handshakeResult)
  }

  def handleInitialMessageV4(data: ByteString): (ByteString, AuthHandshakeResult) = {
    val sizeBytes = data.take(2)
    val encryptedPayload = data.drop(2)

    val plaintext = ECIESCoder.decrypt(
      privKey = nodeKey.getPrivate.asInstanceOf[ECPrivateKeyParameters].getD,
      cipher = encryptedPayload.toArray,
      macData = Some(sizeBytes.toArray)
    )

    val message = {
      import AuthInitiateMessageV4._
      plaintext.toAuthInitiateMessageV4
    }

    val response = AuthResponseMessageV4(
      ephemeralPublicKey = ephemeralKey.getPublic.asInstanceOf[ECPublicKeyParameters].getQ,
      nonce = nonce,
      version = Version
    )
    val encodedResponse = rlp.encode(response)

    val encryptedSize = encodedResponse.length + ECIESCoder.OverheadSize
    val sizePrefix = ByteBuffer.allocate(2).putShort(encryptedSize.toShort).array
    val encryptedResponsePayload = ECIESCoder.encrypt(message.publicKey, secureRandom, encodedResponse, Some(sizePrefix))
    val packet = ByteString(sizePrefix ++ encryptedResponsePayload)

    val remoteEphemeralKey = extractEphemeralKey(message.signature, message.nonce, message.publicKey)

    this.initiatePacketOpt = Some(data)
    this.responsePacketOpt = Some(packet)
    this.remotePubKeyOpt = Some(message.publicKey)
    val handshakeResult = finalizeHandshake(remoteEphemeralKey, message.nonce)

    (packet, handshakeResult)
  }

  private def extractEphemeralKey(signature: ECDSASignature, nonce: ByteString, publicKey: ECPoint): ECPoint = {
    val agreement = new ECDHBasicAgreement
    agreement.init(nodeKey.getPrivate)
    val sharedSecret = agreement.calculateAgreement(new ECPublicKeyParameters(publicKey, crypto.curve))

    val token = bigIntegerToBytes(sharedSecret, NonceSize)
    val signed = xor(token, nonce.toArray)

    val signaturePubBytes = ECDSASignature.recoverPublicKey(signature, signed).get

    crypto.curve.getCurve.decodePoint(ECDSASignature.UncompressedIndicator +: signaturePubBytes)
  }

  private def createAuthInitiateMessageV4(remotePubKey: ECPoint) = {
    val sharedSecret = {
      val agreement = new ECDHBasicAgreement
      agreement.init(nodeKey.getPrivate)
      bigIntegerToBytes(agreement.calculateAgreement(new ECPublicKeyParameters(remotePubKey, crypto.curve)), NonceSize)
    }

    val publicKey = nodeKey.getPublic.asInstanceOf[ECPublicKeyParameters].getQ

    val messageToSign = xor(sharedSecret, nonce.toArray)
    val signature = ECDSASignature.sign(messageToSign, ephemeralKey)

    AuthInitiateMessageV4(signature, publicKey, nonce, Version)
  }

  private def finalizeHandshake(remoteEphemeralKey: ECPoint, remoteNonce: ByteString): AuthHandshakeResult = {
    val successOpt = for {
      initiatePacket <- initiatePacketOpt
      responsePacket <- responsePacketOpt
      remotePubKey <- remotePubKeyOpt
    } yield {
      val secretScalar = {
        val agreement = new ECDHBasicAgreement
        agreement.init(ephemeralKey.getPrivate)
        agreement.calculateAgreement(new ECPublicKeyParameters(remoteEphemeralKey, crypto.curve))
      }

      val agreedSecret = bigIntegerToBytes(secretScalar, SecretSize)

      val sharedSecret = if (isInitiator) {
        crypto.kec256(agreedSecret, crypto.kec256(remoteNonce.toArray, nonce.toArray))
      } else {
        crypto.kec256(agreedSecret, crypto.kec256(nonce.toArray, remoteNonce.toArray))
      }

      val aesSecret = crypto.kec256(agreedSecret, sharedSecret)

      val (egressMacSecret, ingressMacSecret) = if (isInitiator) {
        macSecretSetup(agreedSecret, aesSecret, initiatePacket, nonce, responsePacket, remoteNonce)
      } else {
        macSecretSetup(agreedSecret, aesSecret, initiatePacket, remoteNonce, responsePacket, nonce)
      }

      AuthHandshakeSuccess(
        secrets = new Secrets(
        aes = aesSecret,
        mac = crypto.kec256(agreedSecret, aesSecret),
        token = crypto.kec256(sharedSecret),
        egressMac = egressMacSecret,
        ingressMac = ingressMacSecret
      ),
        remotePubKey = ByteString(remotePubKey.getEncoded(false).tail)
      )
    }

    successOpt getOrElse AuthHandshakeFailure
  }

  private def macSecretSetup(
    agreedSecret:   Array[Byte],
    aesSecret:      Array[Byte],
    initiatePacket: ByteString,
    initiateNonce:  ByteString,
    responsePacket: ByteString,
    responseNonce:  ByteString
  ) = {
    val macSecret = crypto.kec256(agreedSecret, aesSecret)

    val mac1 = new KeccakDigest(MacSize)
    mac1.update(xor(macSecret, responseNonce.toArray), 0, macSecret.length)
    val bufSize = 32
    val buf = new Array[Byte](bufSize)
    new KeccakDigest(mac1).doFinal(buf, 0)
    mac1.update(initiatePacket.toArray, 0, initiatePacket.toArray.length)
    new KeccakDigest(mac1).doFinal(buf, 0)

    val mac2 = new KeccakDigest(MacSize)
    mac2.update(xor(macSecret, initiateNonce.toArray), 0, macSecret.length)
    new KeccakDigest(mac2).doFinal(buf, 0)
    mac2.update(responsePacket.toArray, 0, responsePacket.toArray.length)
    new KeccakDigest(mac2).doFinal(buf, 0)

    if (isInitiator) (mac1, mac2) else (mac2, mac1)
  }

}

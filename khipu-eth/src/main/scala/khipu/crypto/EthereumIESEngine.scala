package khipu.crypto

import akka.util.ByteString
import java.io.ByteArrayInputStream
import org.spongycastle.crypto.BufferedBlockCipher
import org.spongycastle.crypto.Digest
import org.spongycastle.crypto.InvalidCipherTextException
import org.spongycastle.crypto.Mac
import org.spongycastle.crypto.agreement.ECDHBasicAgreement
import org.spongycastle.crypto.generators.ECKeyPairGenerator
import org.spongycastle.crypto.params.ECPrivateKeyParameters
import org.spongycastle.crypto.params.ECPublicKeyParameters
import org.spongycastle.crypto.params.KeyParameter
import org.spongycastle.crypto.params.ParametersWithIV
import org.spongycastle.crypto.parsers.ECIESPublicKeyParser
import org.spongycastle.util.{ Arrays, BigIntegers }

/**
 * Support class for constructing integrated encryption cipher
 * for doing basic message exchanges on top of key agreement ciphers.
 * Follows the description given in IEEE Std 1363a with a couple of changes
 * specific to Ethereum:
 * - Hash the MAC key before use
 * - Include the encryption IV in the MAC computation
 */

/**
 * set up for use with stream mode, where the key derivation function
 * is used to provide a stream of bytes to xor with the message.
 *
 * @param kdf        the key derivation function used for byte generation
 * @param mac        the message authentication code generator for the message
 * @param hash       hash ing function
 * @param cipher     the actual cipher
 * @param IV         vector with random values used to initialize cipher
 * @param prvSrc     private key source
 * @param pubSrc     public key source
 * @param hashMacKey determines if for mac use kdf value (if false) or hashed kdf value (if true)
 */
final class EthereumIESEngine(
    kdf:        Either[ConcatKDFBytesGenerator, MGF1BytesGeneratorExt],
    mac:        Mac,
    hash:       Digest,
    cipher:     Option[BufferedBlockCipher],
    IV:         Option[Array[Byte]],
    prvSrc:     Either[ECPrivateKeyParameters, ECKeyPairGenerator],
    pubSrc:     Either[ECPublicKeyParameters, ECIESPublicKeyParser],
    hashMacKey: Boolean                                                = true
) {

  @throws[InvalidCipherTextException]
  private def encryptBlock(plainText: Array[Byte], inOff: Int, inLen: Int, macData: Option[Array[Byte]],
                           encodedPublicKey: Array[Byte], fillKDFunction: Int => ByteString): Array[Byte] = {

    val (derivedKeySecondPart, cryptogram) = cipher match {

      case Some(cphr) =>
        // Block cipher mode.
        val derivedKey = fillKDFunction(ECIESCoder.KeySize / 8 + ECIESCoder.KeySize / 8)
        val (firstPart, secondPart) = derivedKey.splitAt(ECIESCoder.KeySize / 8)

        IV match {
          case Some(iv) => cphr.init(true, new ParametersWithIV(new KeyParameter(firstPart.toArray), iv))
          case None     => cphr.init(true, new KeyParameter(firstPart.toArray))
        }

        val encrypted = new Array[Byte](cphr.getOutputSize(inLen))
        val len = cphr.processBytes(plainText, inOff, inLen, encrypted, 0)
        cphr.doFinal(encrypted, len)

        (secondPart, ByteString(encrypted))

      case None =>
        // Streaming mode.
        val derivedKey = fillKDFunction(inLen + ECIESCoder.KeySize / 8)
        val (firstPart, secondPart) = derivedKey.splitAt(inLen)

        val encrypted: Seq[Byte] = firstPart.zipWithIndex.map {
          case (value, idx) =>
            (plainText(inOff + idx) ^ value).toByte
        }

        (secondPart, ByteString(encrypted: _*))
    }

    // calculate mac
    mac.init(new KeyParameter(getKdfForMac(derivedKeySecondPart)))
    IV.foreach(iv => mac.update(iv, 0, iv.length))
    mac.update(cryptogram.toArray, 0, cryptogram.length)

    macData.foreach(data => mac.update(data, 0, data.length))

    val messageAuthenticationCode = new Array[Byte](mac.getMacSize)
    mac.doFinal(messageAuthenticationCode, 0)

    encodedPublicKey ++ cryptogram ++ messageAuthenticationCode
  }

  @throws[InvalidCipherTextException]
  private def decryptBlock(cryptogram: Array[Byte], inOff: Int, inLen: Int, macData: Option[Array[Byte]],
                           encodedPublicKey: Array[Byte], fillKDFunction: Int => ByteString): Array[Byte] = {

    // Ensure that the length of the input is greater than the MAC in bytes
    if (inLen <= (ECIESCoder.KeySize / 8)) throw new InvalidCipherTextException("Length of input must be greater than the MAC")

    val (derivedKeySecondPart, plainText) = cipher match {
      case Some(cphr) =>
        // Block cipher mode.
        val derivedKey: ByteString = fillKDFunction(ECIESCoder.KeySize / 8 + ECIESCoder.KeySize / 8)
        val (firstPart, secondPart) = derivedKey.splitAt(ECIESCoder.KeySize / 8)

        IV match {
          case Some(iv) => cphr.init(false, new ParametersWithIV(new KeyParameter(firstPart.toArray), iv))
          case None     => cphr.init(false, new KeyParameter(firstPart.toArray))
        }

        val decrypted = new Array[Byte](cphr.getOutputSize(inLen - encodedPublicKey.length - mac.getMacSize))
        val len = cphr.processBytes(cryptogram, inOff + encodedPublicKey.length, inLen - encodedPublicKey.length - mac.getMacSize, decrypted, 0)
        cphr.doFinal(decrypted, len)

        (secondPart, ByteString(decrypted))
      case None =>
        // Streaming mode.
        val derivedKey = fillKDFunction((inLen - encodedPublicKey.length - mac.getMacSize) + (ECIESCoder.KeySize / 8))
        val (firstPart, secondPart) = derivedKey.splitAt(inLen - encodedPublicKey.length - mac.getMacSize)

        val decrypted: Seq[Byte] = firstPart.zipWithIndex.map {
          case (value, idx) =>
            (cryptogram(inOff + encodedPublicKey.length + idx) ^ value).toByte
        }

        (secondPart, ByteString(decrypted: _*))
    }

    val end = inOff + inLen
    val messageAuthenticationCode = Arrays.copyOfRange(cryptogram, end - mac.getMacSize, end)
    val messageAuthenticationCodeCalculated = new Array[Byte](messageAuthenticationCode.length)

    mac.init(new KeyParameter(getKdfForMac(derivedKeySecondPart)))
    IV.foreach(iv => mac.update(iv, 0, iv.length))
    mac.update(cryptogram, inOff + encodedPublicKey.length, inLen - encodedPublicKey.length - messageAuthenticationCodeCalculated.length)

    macData foreach { data => mac.update(data, 0, data.length) }
    mac.doFinal(messageAuthenticationCodeCalculated, 0)

    if (!Arrays.constantTimeAreEqual(messageAuthenticationCode, messageAuthenticationCodeCalculated))
      throw new InvalidCipherTextException("Invalid MAC.")

    plainText.toArray
  }

  private def getKdfForMac(derivedKeySecondPart: ByteString) = if (hashMacKey) {
    val hashBuff = new Array[Byte](hash.getDigestSize)
    hash.reset()
    hash.update(derivedKeySecondPart.toArray, 0, derivedKeySecondPart.length)
    hash.doFinal(hashBuff, 0)
    hashBuff
  } else {
    derivedKeySecondPart.toArray
  }

  @throws[InvalidCipherTextException]
  def processBlock(in: Array[Byte], inOff: Int, inLen: Int, forEncryption: Boolean, macData: Option[Array[Byte]] = None): Array[Byte] = {
    val (prv, encodedEphKeyPair) = prvSrc.fold(
      key => (key, None),
      keyPairGenerator => {
        val ephKeyPair = keyPairGenerator.generateKeyPair()
        val prvParam = ephKeyPair.getPrivate.asInstanceOf[ECPrivateKeyParameters]
        val pubEncodedParam = ephKeyPair.getPublic.asInstanceOf[ECPublicKeyParameters].getQ.getEncoded(false)
        (prvParam, Some(pubEncodedParam))
      }
    )

    val (pub, encodedPublicKey) = pubSrc.fold(
      key => (key, None),
      keyParser => {
        val bIn = new ByteArrayInputStream(in, inOff, inLen)
        val result = keyParser.readKey(bIn).asInstanceOf[ECPublicKeyParameters]
        val encLength = inLen - bIn.available
        (result, Some(Arrays.copyOfRange(in, inOff, inOff + encLength)))
      }
    )

    val agree = new ECDHBasicAgreement
    agree.init(prv)
    val sharedSecret = BigIntegers.asUnsignedByteArray(agree.getFieldSize, agree.calculateAgreement(pub))

    val fillKDFunction = (outLen: Int) => kdf.fold(_.generateBytes(outLen, sharedSecret), _.generateBytes(outLen, sharedSecret))

    val encodedKey = encodedPublicKey.orElse(encodedEphKeyPair).getOrElse(new Array[Byte](0))

    if (forEncryption) encryptBlock(in, inOff, inLen, macData, encodedKey, fillKDFunction)
    else decryptBlock(in, inOff, inLen, macData, encodedKey, fillKDFunction)
  }
}

package khipu.crypto

import java.io.{ ByteArrayInputStream, IOException }
import java.math.BigInteger
import java.security.SecureRandom
import org.spongycastle.crypto.digests.{ SHA1Digest, SHA256Digest }
import org.spongycastle.crypto.engines.AESEngine
import org.spongycastle.crypto.generators.ECKeyPairGenerator
import org.spongycastle.crypto.macs.HMac
import org.spongycastle.crypto.modes.SICBlockCipher
import org.spongycastle.crypto.params._
import org.spongycastle.crypto.parsers.ECIESPublicKeyParser
import org.spongycastle.crypto.{ BufferedBlockCipher, InvalidCipherTextException }
import org.spongycastle.math.ec.ECPoint

object ECIESCoder {

  val KeySize = 128
  val PublicKeyOverheadSize = 65
  val MacOverheadSize = 32
  val OverheadSize = PublicKeyOverheadSize + KeySize / 8 + MacOverheadSize

  @throws[IOException]
  @throws[InvalidCipherTextException]
  def decrypt(privKey: BigInteger, cipher: Array[Byte], macData: Option[Array[Byte]] = None): Array[Byte] = {
    val is = new ByteArrayInputStream(cipher)
    val ephemBytes = new Array[Byte](2 * ((curve.getCurve.getFieldSize + 7) / 8) + 1)
    is.read(ephemBytes)
    val ephem = curve.getCurve.decodePoint(ephemBytes)
    val IV = new Array[Byte](KeySize / 8)
    is.read(IV)
    val cipherBody = new Array[Byte](is.available)
    is.read(cipherBody)
    decrypt(ephem, privKey, Some(IV), cipherBody, macData)
  }

  @throws[InvalidCipherTextException]
  def decrypt(ephem: ECPoint, prv: BigInteger, IV: Option[Array[Byte]], cipher: Array[Byte], macData: Option[Array[Byte]]): Array[Byte] = {
    val aesEngine = new AESEngine

    val iesEngine = new EthereumIESEngine(
      kdf = Left(new ConcatKDFBytesGenerator(new SHA256Digest)),
      mac = new HMac(new SHA256Digest),
      hash = new SHA256Digest,
      cipher = Some(new BufferedBlockCipher(new SICBlockCipher(aesEngine))),
      IV = IV,
      prvSrc = Left(new ECPrivateKeyParameters(prv, curve)),
      pubSrc = Left(new ECPublicKeyParameters(ephem, curve))
    )

    iesEngine.processBlock(cipher, 0, cipher.length, forEncryption = false, macData)
  }

  /**
   * Encryption equivalent to the Crypto++ default ECIES<ECP> settings:
   *
   * DL_KeyAgreementAlgorithm:        DL_KeyAgreementAlgorithm_DH<struct ECPPoint,struct EnumToType<enum CofactorMultiplicationOption,0> >
   * DL_KeyDerivationAlgorithm:       DL_KeyDerivationAlgorithm_P1363<struct ECPPoint,0,class P1363_KDF2<class SHA1> >
   * DL_SymmetricEncryptionAlgorithm: DL_EncryptionAlgorithm_Xor<class HMAC<class SHA1>,0>
   * DL_PrivateKey:                   DL_Key<ECPPoint>
   * DL_PrivateKey_EC<class ECP>
   *
   * Used for Whisper V3
   */
  @throws[IOException]
  @throws[InvalidCipherTextException]
  def decryptSimple(privKey: BigInteger, cipher: Array[Byte]): Array[Byte] = {
    val iesEngine = new EthereumIESEngine(
      kdf = Right(new MGF1BytesGeneratorExt(new SHA1Digest)),
      mac = new HMac(new SHA1Digest),
      hash = new SHA1Digest,
      cipher = None,
      IV = Some(new Array[Byte](0)),
      prvSrc = Left(new ECPrivateKeyParameters(privKey, curve)),
      pubSrc = Right(new ECIESPublicKeyParser(curve)),
      hashMacKey = false
    )

    iesEngine.processBlock(cipher, 0, cipher.length, forEncryption = false)
  }

  def encrypt(toPub: ECPoint, secureRandom: SecureRandom, plaintext: Array[Byte], macData: Option[Array[Byte]] = None): Array[Byte] = {

    val gParam = new ECKeyGenerationParameters(curve, secureRandom)

    val IV = secureRandomByteArray(secureRandom, KeySize / 8)

    val eGen = new ECKeyPairGenerator
    eGen.init(gParam)
    val ephemPair = eGen.generateKeyPair

    val prv = ephemPair.getPrivate.asInstanceOf[ECPrivateKeyParameters].getD
    val pub = ephemPair.getPublic.asInstanceOf[ECPublicKeyParameters].getQ

    val iesEngine = makeIESEngine(toPub, prv, Some(IV))

    val keygenParams = new ECKeyGenerationParameters(curve, secureRandom)
    val generator = new ECKeyPairGenerator
    generator.init(keygenParams)
    val gen = new ECKeyPairGenerator
    gen.init(new ECKeyGenerationParameters(curve, secureRandom))

    pub.getEncoded(false) ++ IV ++ iesEngine.processBlock(plaintext, 0, plaintext.length, forEncryption = true, macData)
  }

  /**
   * Encryption equivalent to the Crypto++ default ECIES<ECP> settings:
   *
   * DL_KeyAgreementAlgorithm:        DL_KeyAgreementAlgorithm_DH<struct ECPPoint,struct EnumToType<enum CofactorMultiplicationOption,0> >
   * DL_KeyDerivationAlgorithm:       DL_KeyDerivationAlgorithm_P1363<struct ECPPoint,0,class P1363_KDF2<class SHA1> >
   * DL_SymmetricEncryptionAlgorithm: DL_EncryptionAlgorithm_Xor<class HMAC<class SHA1>,0>
   * DL_PrivateKey:                   DL_Key<ECPPoint>
   * DL_PrivateKey_EC<class ECP>
   *
   * Used for Whisper V3
   */
  @throws[IOException]
  @throws[InvalidCipherTextException]
  def encryptSimple(pub: ECPoint, secureRandom: SecureRandom, plaintext: Array[Byte]): Array[Byte] = {

    val eGen = new ECKeyPairGenerator
    val gParam = new ECKeyGenerationParameters(curve, secureRandom)
    eGen.init(gParam)

    val iesEngine = new EthereumIESEngine(
      kdf = Right(new MGF1BytesGeneratorExt(new SHA1Digest)),
      mac = new HMac(new SHA1Digest),
      hash = new SHA1Digest,
      cipher = None,
      IV = Some(new Array[Byte](0)),
      prvSrc = Right(eGen),
      pubSrc = Left(new ECPublicKeyParameters(pub, curve)),
      hashMacKey = false
    )

    iesEngine.processBlock(plaintext, 0, plaintext.length, forEncryption = true)
  }

  private def makeIESEngine(pub: ECPoint, prv: BigInteger, IV: Option[Array[Byte]]) = {
    val aesEngine = new AESEngine

    val iesEngine = new EthereumIESEngine(
      kdf = Left(new ConcatKDFBytesGenerator(new SHA256Digest)),
      mac = new HMac(new SHA256Digest),
      hash = new SHA256Digest,
      cipher = Some(new BufferedBlockCipher(new SICBlockCipher(aesEngine))),
      IV = IV,
      prvSrc = Left(new ECPrivateKeyParameters(prv, curve)),
      pubSrc = Left(new ECPublicKeyParameters(pub, curve))
    )

    iesEngine
  }

}

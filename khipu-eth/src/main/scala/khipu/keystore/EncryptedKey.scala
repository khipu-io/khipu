package khipu.keystore

import akka.util.ByteString
import java.security.SecureRandom
import java.util.UUID
import khipu.crypto
import khipu.crypto.SymmetricCipher
import khipu.domain.Address
import khipu.keystore.EncryptedKey._

object EncryptedKey {
  val AES128CTR = "aes-128-ctr"
  val AES128CBC = "aes-128-cbc"
  val Scrypt = "scrypt"
  val Pbkdf2 = "pbkdf2"

  sealed trait KdfParams
  final case class ScryptParams(salt: ByteString, n: Int, r: Int, p: Int, dklen: Int) extends KdfParams
  final case class Pbkdf2Params(salt: ByteString, prf: String, c: Int, dklen: Int) extends KdfParams

  final case class CryptoSpec(
    cipher:     String,
    ciphertext: ByteString,
    iv:         ByteString,
    kdfParams:  KdfParams,
    mac:        ByteString
  )

  def apply(prvKey: ByteString, passphrase: String, secureRandom: SecureRandom): EncryptedKey = {
    val version = 3
    val uuid = UUID.randomUUID()
    val pubKey = crypto.pubKeyFromPrvKey(prvKey)
    val address = Address(crypto.kec256(pubKey))

    val salt = crypto.secureRandomByteString(secureRandom, 32)
    val kdfParams = ScryptParams(salt, 1 << 18, 8, 1, 32) //params used by Geth
    val dk = deriveKey(passphrase, kdfParams)

    val cipherName = AES128CTR
    val iv = crypto.secureRandomByteString(secureRandom, 16)
    val secret = dk.take(16)
    val ciphertext = getCipher(cipherName).encrypt(secret, iv, prvKey)

    val mac = createMac(dk, ciphertext)

    val cryptoSpec = CryptoSpec(cipherName, ciphertext, iv, kdfParams, mac)
    EncryptedKey(uuid, address, cryptoSpec, version)
  }

  private def getCipher(cipherName: String): SymmetricCipher =
    Map(AES128CBC -> crypto.AES_CBC, AES128CTR -> crypto.AES_CTR)(cipherName.toLowerCase)

  private def deriveKey(passphrase: String, kdfParams: KdfParams): ByteString =
    kdfParams match {
      case ScryptParams(salt, n, r, p, dklen) =>
        crypto.scrypt(passphrase, salt, n, r, p, dklen)

      case Pbkdf2Params(salt, prf, c, dklen) =>
        // prf is currently ignored, only hmac sha256 is used
        crypto.pbkdf2HMacSha256(passphrase, salt, c, dklen)
    }

  private def createMac(dk: ByteString, ciphertext: ByteString): ByteString =
    ByteString(crypto.kec256(dk.slice(16, 32) ++ ciphertext))
}

/**
 * Represents an encrypted private key stored in the keystore
 * See: https://github.com/ethereum/wiki/wiki/Web3-Secret-Storage-Definition
 */
final case class EncryptedKey(
    id:         UUID,
    address:    Address,
    cryptoSpec: CryptoSpec,
    version:    Int
) {

  def decrypt(passphrase: String): Either[String, ByteString] = {
    val dk = deriveKey(passphrase, cryptoSpec.kdfParams)
    val secret = dk.take(16)
    val decrypted = getCipher(cryptoSpec.cipher).decrypt(secret, cryptoSpec.iv, cryptoSpec.ciphertext)
    decrypted
      .filter(_ => createMac(dk, cryptoSpec.ciphertext) == cryptoSpec.mac)
      .map(Right(_))
      .getOrElse(Left("Couldn't decrypt key with given passphrase"))
  }
}

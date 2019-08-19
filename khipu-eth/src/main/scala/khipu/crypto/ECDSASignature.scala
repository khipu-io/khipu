package khipu.crypto

import akka.util.ByteString
import java.math.BigInteger
import java.util.Arrays
import khipu.config.BlockchainConfig
import khipu.config.KhipuConfig
import khipu.util
import org.spongycastle.asn1.x9.X9IntegerConverter
import org.spongycastle.crypto.AsymmetricCipherKeyPair
import org.spongycastle.crypto.digests.SHA256Digest
import org.spongycastle.crypto.params.ECPublicKeyParameters
import org.spongycastle.crypto.signers.ECDSASigner
import org.spongycastle.crypto.signers.HMacDSAKCalculator
import org.spongycastle.math.ec.ECAlgorithms
import org.spongycastle.math.ec.ECCurve
import org.spongycastle.math.ec.ECPoint

/**
 * EIP 155 fork starts at 2675000
 * EIP 155: Replay attack protection – prevents transactions from one Ethereum
 * chain from being rebroadcasted on an alternative chain. For example: If you
 * send 150 test ether to someone from the Morden testnet, that same
 * transaction cannot be replayed on the main Ethereum chain. Important note:
 *
 * EIP 155 is backwards compatible, so transactions generated with the
 * “pre-Spurious-Dragon” format will still be accepted. However, to ensure you
 * are protected against replay attacks, you will still need to use a wallet
 * solution that implements EIP 155.
 *
 * Be aware that this backwards compatibility also means that transactions
 * created from alternative Ethereum based blockchains that have not
 * implemented EIP 155 (such as Ethereum Classic) can still be replayed on the
 * main Ethereum chain.
 *
 * https://github.com/ethereum/eips/issues/155
 * https://github.com/ethereum/EIPs/blob/master/EIPS/eip-155.md
 *
 * ## Preamble
 * ```
 * EIP: 155
 * Title: Simple replay attack protection
 * Author: Vitalik Buterin
 * Type: Standard Track
 * Category: Core
 * Status: Final
 * Created: 2016-10-14
 * ```
 *
 * ### Parameters
 * - `FORK_BLKNUM`: TBA
 * - `CHAIN_ID`: 1
 * ### Specification
 *
 * If `block.number >= FORK_BLKNUM` and `v = CHAIN_ID * 2 + 35` or `v = CHAIN_ID * 2 + 36`,
 * then when computing the hash of a transaction for purposes of signing or
 * recovering, instead of hashing only the first six elements (ie. nonce,
 * gasprice, startgas, to, value, data), hash nine elements, with `v` replaced
 * by `CHAIN_ID`, `r = 0` and `s = 0`. The currently existing signature scheme
 * using `v = 27` and `v = 28` remains valid and continues to operate under
 * the same rules as it does now.
 *
 * ### Example
 *
 * Consider a transaction with `nonce = 9`, `gasprice = 20 * 10**9`, `startgas = 21000`, `to = 0x3535353535353535353535353535353535353535`, `value = 10**18`, `data=''` (empty).
 *
 * The "signing data" becomes:
 *
 * ```
 * 0xec098504a817c800825208943535353535353535353535353535353535353535880de0b6b3a764000080018080
 * ```
 *
 * The "signing hash" becomes:
 *
 * ```
 * 0xdaf5a779ae972f972197303d7b574746c7ef83eadac0f2791ad23db92e4c8e53
 * ```
 *
 * If the transaction is signed with the private key `0x4646464646464646464646464646464646464646464646464646464646464646`, then the v,r,s values become:
 *
 * ```
 * (37, 18515461264373351373200002665853028612451056578545711640558177340181847433846, 46948507304638947509940763649030358759909902576025900602547168820602576006531)
 * ```
 *
 * Notice the use of 37 instead of 27. The signed tx would become:
 *
 * ```
 * 0xf86c098504a817c800825208943535353535353535353535353535353535353535880de0b6b3a76400008025a028ef61340bd939bc2195fe537567866003e1a15d3c71ff63e1590620aa636276a067cbe9d8997f761aecb703304b3800ccf555c9f3dc64214b297fb1966a3b6d83
 * ```
 * ### Rationale
 *
 * This would provide a way to send transactions that work on ethereum without
 * working on ETC or the Morden testnet. ETC is encouraged to adopt this EIP
 * but replacing `CHAIN_ID` with a different value, and all future testnets,
 * consortium chains and alt-etherea are encouraged to adopt this EIP
 * replacing `CHAIN_ID` with a unique value.
 *
 *
 * ### List of Chain ID's:
 *
 * | `CHAIN_ID`     | Chain(s)                                   |
 * | ---------------| -------------------------------------------|
 * | 1              | Ethereum mainnet                           |
 * | 2              | Morden (disused), Expanse mainnet          |
 * | 3              | Ropsten                                    |
 * | 4              | Rinkeby                                    |
 * | 30             | Rootstock mainnet                          |
 * | 31             | Rootstock testnet                          |
 * | 42             | Kovan                                      |
 * | 61             | Ethereum Classic mainnet                   |
 * | 62             | Ethereum Classic testnet                   |
 * | 1337           | Geth private chains (default)              |
 *
 */
object ECDSASignature {

  val isEth = KhipuConfig.chainType match {
    case "eth" => true
    case _     => false
  }

  val SLength = 32
  val RLength = 32
  val VLength = 1
  val EncodedLength = RLength + SLength + VLength

  // byte value that indicates that bytes representing ECC point are in uncompressed format, and should be decoded properly
  val UncompressedIndicator: Byte = 0x04

  val NegativePointSign: Byte = 27
  val PositivePointSign: Byte = 28
  val EIP155NegativePointSign: Byte = 35
  val EIP155PositivePointSign: Byte = 36

  val allowedPointSigns = Set(NegativePointSign, PositivePointSign)

  val HalfCurveOrder = curveParams.getN.shiftRight(1)

  def nodeIdFromPublicKey(pubKey: ECPublicKeyParameters): Array[Byte] =
    pubKey.getQ.getEncoded(false).drop(1) // drop type info

  def publicKeyFromNodeId(nodeId: String): ECPoint =
    curve.getCurve.decodePoint(UncompressedIndicator +: khipu.hexDecode(nodeId))

  // --- choose eth or etc
  def extractChainIdFromV(v: Int): Option[Int] =
    if (isEth) extractChainIdFromV_eth(v) else extractChainIdFromV_etc(v)

  def getRealV(v: Int): Byte =
    if (isEth) getRealV_eth(v) else getRealV_etc(v)

  // TODO, ETH return Int, ETC should return Byte (for example of chainId 61, the v (2*61+35) is int 157 but byte -99)
  //def getEncodeV(v: Byte, chainId: Option[Int]): Int =
  def getEncodeV(v: Byte, chainId: Option[Int]): Either[Byte, Int] =
    if (isEth) Right(getEncodeV_eth(v, chainId)) else Left(getEncodeV_etc(v, chainId))

  def sign(message: Array[Byte], keyPair: AsymmetricCipherKeyPair, chainId: Option[Int] = None): ECDSASignature =
    if (isEth) sign_eth(message, keyPair, chainId) else sign_etc(message, keyPair, chainId)

  /**
   * returns ECC point encoded with on compression and without leading byte indicating compression
   * @param message message to be signed
   * @param chainId optional value if you want new signing schema with recovery id calculated with chain id
   * @return
   */
  def recoverPublicKey(sig: ECDSASignature, message: ByteString): Option[ByteString] = recoverPublicKey(sig, message.toArray, None).map(ByteString(_))
  def recoverPublicKey(sig: ECDSASignature, message: Array[Byte], chainId: Option[Int] = None): Option[Array[Byte]] =
    if (isEth) recoverPubBytes_eth(sig.r, sig.s, sig.v, message, chainId) else recoverPubBytes_etc(sig.r, sig.s, sig.v, message, chainId)
  // --- end of choose eth or etc

  private def sign_etc(message: Array[Byte], keyPair: AsymmetricCipherKeyPair, chainId: Option[Int] = None): ECDSASignature = {
    val signer = new ECDSASigner(new HMacDSAKCalculator(new SHA256Digest))
    signer.init(true, keyPair.getPrivate)
    val components = signer.generateSignature(message)
    val r = components(0)
    val s = canonicalise_etc(components(1))
    val v = calculateV_etc(r, s, keyPair, message).getOrElse(throw new RuntimeException("Failed to calculate signature rec id"))

    val pointSign = chainId match {
      case Some(id) if v == NegativePointSign => (id * 2 + EIP155NegativePointSign).toByte
      case Some(id) if v == PositivePointSign => (id * 2 + EIP155PositivePointSign).toByte
      case None                               => v
    }

    ECDSASignature(r, s, pointSign)
  }

  private def sign_eth(message: Array[Byte], keyPair: AsymmetricCipherKeyPair, chainId: Option[Int] = None): ECDSASignature = {
    val signer = new ECDSASigner(new HMacDSAKCalculator(new SHA256Digest))
    signer.init(true, keyPair.getPrivate)
    val components = signer.generateSignature(message)
    val r = components(0)
    val s = canonicalise_eth(components(1))
    val v = calculateV_eth(r, s, keyPair, message).getOrElse(throw new RuntimeException("Failed to calculate signature rec id"))

    ECDSASignature(r, s, v)
  }

  private def extractChainIdFromV_etc(v: Int): Option[Int] = {
    Some(BlockchainConfig(KhipuConfig.config).chainId.toByte)
  }

  /**
   * Since EIP-155, we could encode chainId in V
   */
  private def extractChainIdFromV_eth(v: Int): Option[Int] = {
    //if (v.bitLength() > 31) return Integer.MAX_VALUE; // chainId is limited to 31 bits, longer are not valid for now
    v match {
      case NegativePointSign | PositivePointSign => None
      case _                                     => Some((v - EIP155NegativePointSign) / 2)
    }
  }

  private def getRealV_etc(v: Int): Byte = {
    v.toByte
  }

  /**
   * @return 27 or 28
   */
  private def getRealV_eth(v: Int): Byte = {
    //if (v.bitLength() > 31) return 0; // chainId is limited to 31 bits, longer are not valid for now
    v match {
      case NegativePointSign | PositivePointSign => v.toByte
      case _                                     => if (v % 2 == 0) PositivePointSign else NegativePointSign
    }
  }

  private def getEncodeV_etc(v: Byte, chainId: Option[Int]): Byte = {
    v
  }

  private def getEncodeV_eth(v: Byte, chainId: Option[Int]): Int = {
    chainId match {
      case Some(id) =>
        // v - NegativePointSign ------------ 0 or 1
        // + id * 2 + NewNegativePointSign
        v match {
          case NegativePointSign => id * 2 + EIP155NegativePointSign
          case PositivePointSign => id * 2 + EIP155PositivePointSign
          case _                 => id * 2 + EIP155NegativePointSign + (v - NegativePointSign)
        }
      case None => v
    }
  }

  /**
   * New formula for calculating point sign post EIP 155 adoption
   * v = CHAIN_ID * 2 + 35 or v = CHAIN_ID * 2 + 36
   */
  private def getRecoveredPointSign_etc(pointSign: Byte, chainId: Option[Int]): Option[Byte] = {
    (chainId match {
      case Some(id) =>
        if (pointSign == NegativePointSign || pointSign == (id * 2 + EIP155NegativePointSign).toByte) {
          Some(NegativePointSign)
        } else if (pointSign == PositivePointSign || pointSign == (id * 2 + EIP155PositivePointSign).toByte) {
          Some(PositivePointSign)
        } else {
          None
        }
      case None => Some(pointSign)
    }).filter(allowedPointSigns.contains)
  }

  private def getRecoverIdx_eth(v: Int): Option[Int] = {
    // The header byte: 0x1B = 1st key with even y, 0x1C = 1st key with odd y,
    //                  0x1D = 2nd key with even y, 0x1E = 2nd key with odd y
    if (v >= 27 && v <= 34) { // 27,28,29,30,31,32,33,34
      if (v >= 31) { // 31,32,33,34
        Some(v - 31) // 0,1,2,3
      } else { // 27,28,29,30
        Some(v - 27) // 0,1,2,3
      }
    } else {
      None // "Header byte out of range: "
    }
  }

  /**
   * Will automatically adjust the S component to be less than or equal to half the curve order, if necessary.
   * This is required because for every signature (r,s) the signature (r, -s (mod N)) is a valid signature of
   * the same message. However, we dislike the ability to modify the bits of a Ethereum transaction after it's
   * been signed, as that violates various assumed invariants. Thus in future only one of those forms will be
   * considered legal and the other will be banned.
   *
   * @return  -
   */
  private def canonicalise_etc(s: BigInteger): BigInteger = {
    // The order of the curve is the number of valid points that exist on that curve. If S is in the upper
    // half of the number of valid points, then bring it back to the lower half. Otherwise, imagine that
    //    N = 10
    //    s = 8, so (-8 % 10 == 2) thus both (r, 8) and (r, 2) are valid solutions.
    //    10 - 8 == 2, giving us always the latter solution, which is canonical.
    if (s.compareTo(HalfCurveOrder) > 0) curve.getN subtract s else s
  }

  private def canonicalise_eth(s: BigInteger): BigInteger = canonicalise_etc(s)

  private def calculateV_etc(r: BigInteger, s: BigInteger, key: AsymmetricCipherKeyPair, messageHash: Array[Byte]): Option[Byte] = {
    // byte 0 of encoded ECC point indicates that it is uncompressed point, it is part of spongycastle encoding
    val pubKey = key.getPublic.asInstanceOf[ECPublicKeyParameters].getQ.getEncoded(false).tail
    Seq(PositivePointSign, NegativePointSign).find { i =>
      recoverPubBytes_etc(r, s, i, messageHash, None).exists(Arrays.equals(_, pubKey))
    }
  }

  private def calculateV_eth(r: BigInteger, s: BigInteger, key: AsymmetricCipherKeyPair, messageHash: Array[Byte]): Option[Byte] = {
    // byte 0 of encoded ECC point indicates that it is uncompressed point, it is part of spongycastle encoding
    val pubKey = key.getPublic.asInstanceOf[ECPublicKeyParameters].getQ.getEncoded(false).tail
    // Now we have to work backwards to figure out the recId needed to recover the signature.
    var recId = -1
    var i = 0
    var break = false
    while (i < 4 && !break) {
      recoverPubBytesForRecoverId_eth(r, s, i.toByte, messageHash) match {
        case Some(key) if Arrays.equals(key, pubKey) =>
          recId = i
          break = true
        case _ =>
          i += 1
      }
    }

    if (recId == -1) {
      None
    } else {
      Some((recId + 27).toByte)
    }
  }

  /**
   * <p>Given the components of a signature and a selector value, recover and return the public key
   * that generated the signature according to the algorithm in SEC1v2 section 4.1.6.</p>
   *
   * <p>The recoverId is an index from 0 to 3 which indicates which of the 4 possible keys is the correct one. Because
   * the key recovery operation yields multiple potential keys, the correct key must either be stored alongside the
   * signature, or you must be willing to try each recId in turn until you find one that outputs the key you are
   * expecting.</p>
   *
   * <p>If this method returns null it means recovery was not possible and recId should be iterated.</p>
   *
   * <p>Given the above two points, a correct usage of this method is inside a for loop from 0 to 3, and if the
   * output is null OR a key that is not the one you expect, you try again with the next recId.</p>
   *
   * @param sig the R and S components of the signature, wrapped.
   * @param recoverId Which possible key to recover.
   * @patam chainId
   * @param messageHash Hash of the data that was signed.
   * @return 65-byte encoded public key
   */
  private def recoverPubBytes_etc(r: BigInteger, s: BigInteger, v: Byte, messageHash: Array[Byte], chainId: Option[Int]): Option[Array[Byte]] = {
    getRecoveredPointSign_etc(v, chainId) flatMap { recoverPointSign =>
      val order = curve.getCurve.getOrder
      // ignore case when x = r + order because it is negligibly improbable
      // says: https://github.com/paritytech/rust-secp256k1/blob/f998f9a8c18227af200f0f7fdadf8a6560d391ff/depend/secp256k1/src/ecdsa_impl.h#L282
      val xCoordinate = r
      val curveFp = curve.getCurve.asInstanceOf[ECCurve.Fp]
      val prime = curveFp.getQ

      if (xCoordinate.compareTo(prime) < 0) {
        val R = constructPoint(xCoordinate, recoverPointSign == PositivePointSign)
        if (R.multiply(order).isInfinity) {
          val e = new BigInteger(1, messageHash)
          val rInv = r.modInverse(order)
          // Q = r^(-1)(sR - eG)
          val q = R.multiply(s).subtract(curve.getG.multiply(e)).multiply(rInv)
          // byte 0 of encoded ECC point indicates that it is uncompressed point, it is part of spongycastle encoding
          Some(q.getEncoded(false).tail)
        } else None
      } else None
    }
  }

  private def recoverPubBytes_eth(r: BigInteger, s: BigInteger, v: Int, message: Array[Byte], chainId: Option[Int]): Option[Array[Byte]] = {
    getRecoverIdx_eth(v) flatMap { recoverIdx =>
      recoverPubBytesForRecoverId_eth(r, s, recoverIdx, message)
    }
  }

  /**
   * <p>Given the components of a signature and a selector value, recover and return the public key
   * that generated the signature according to the algorithm in SEC1v2 section 4.1.6.</p>
   *
   * <p>The recId is an index from 0 to 3 which indicates which of the 4 possible keys is the correct one. Because
   * the key recovery operation yields multiple potential keys, the correct key must either be stored alongside the
   * signature, or you must be willing to try each recId in turn until you find one that outputs the key you are
   * expecting.</p>
   *
   * <p>If this method returns null it means recovery was not possible and recId should be iterated.</p>
   *
   * <p>Given the above two points, a correct usage of this method is inside a for loop from 0 to 3, and if the
   * output is null OR a key that is not the one you expect, you try again with the next recId.</p>
   *
   * @param recId Which possible key to recover.
   * @param sig the R and S components of the signature, wrapped.
   * @param messageHash Hash of the data that was signed.
   * @return 65-byte encoded public key
   */
  private def recoverPubBytesForRecoverId_eth(r: BigInteger, s: BigInteger, recoverIdx: Int, messageHash: Array[Byte]): Option[Array[Byte]] = {
    //check(recId >= 0, "recId must be positive");
    //check(sig.r.signum() >= 0, "r must be positive");
    //check(sig.s.signum() >= 0, "s must be positive");
    //check(messageHash != null, "messageHash must not be null");
    // 1.0 For j from 0 to h   (h == recIdx here and the loop is outside this function)
    //   1.1 Let x = r + jn
    val n = curve.getN // Curve order.
    val i = BigInteger.valueOf(recoverIdx / 2)
    val x = r.add(i.multiply(n))
    //   1.2. Convert the integer x to an octet string X of length mlen using the conversion routine
    //        specified in Section 2.3.7, where mlen = ⌈(log2 p)/8⌉ or mlen = ⌈m/8⌉.
    //   1.3. Convert the octet string (16 set binary digits)||X to an elliptic curve point R using the
    //        conversion routine specified in Section 2.3.4. If this conversion routine outputs “invalid”, then
    //        do another iteration of Step 1.
    //
    // More concisely, what these points mean is to use X as a compressed public key.
    val curveFp = curve.getCurve.asInstanceOf[ECCurve.Fp]
    val prime = curveFp.getQ // Bouncy Castle is not consistent about the letter it uses for the prime.
    if (x.compareTo(prime) < 0) {
      // Compressed keys require you to know an extra bit of data about the y-coord as there are two possibilities.
      // So it's encoded in the recId.
      val R = constructPoint(x, (recoverIdx & 1) == 1)
      //   1.4. If nR != point at infinity, then do another iteration of Step 1 (callers responsibility).
      if (R.multiply(n).isInfinity) {
        //   1.5. Compute e from M using Steps 2 and 3 of ECDSA signature verification.
        val e = new BigInteger(1, messageHash)
        //   1.6. For k from 1 to 2 do the following.   (loop is outside this function via iterating recId)
        //   1.6.1. Compute a candidate public key as:
        //               Q = mi(r) * (sR - eG)
        //
        // Where mi(x) is the modular multiplicative inverse. We transform this into the following:
        //               Q = (mi(r) * s ** R) + (mi(r) * -e ** G)
        // Where -e is the modular additive inverse of e, that is z such that z + e = 0 (mod n). In the above equation
        // ** is point multiplication and + is point addition (the EC group operator).
        //
        // We can find the additive inverse by subtracting e from zero then taking the mod. For example the additive
        // inverse of 3 modulo 11 is 8 because 3 + 8 mod 11 = 0, and -3 mod 11 = 8.
        val eInv = BigInteger.ZERO.subtract(e).mod(n)
        val rInv = r.modInverse(n)
        val srInv = rInv.multiply(s).mod(n)
        val eInvrInv = rInv.multiply(eInv).mod(n)
        val q = ECAlgorithms.sumOfTwoMultiplies(curve.getG, eInvrInv, R, srInv).asInstanceOf[ECPoint.Fp]
        // byte 0 of encoded ECC point indicates that it is uncompressed point, it is part of spongycastle encoding
        Some(q.getEncoded(false).tail)
      } else {
        None
      }
    } else {
      // Cannot have point co-ordinates larger than this as everything takes place modulo Q.
      None
    }
  }

  /**
   * Decompress a compressed public key (x co-ord and low-bit of y-coord).
   *
   * @param xBN -
   * @param yBit -
   * @return -
   */
  private def constructPoint(xBN: BigInteger, yBit_PositivePointSign: Boolean): ECPoint = {
    val x9 = new X9IntegerConverter()
    val compEnc = x9.integerToBytes(xBN, 1 + x9.getByteLength(curve.getCurve))
    compEnc(0) = if (yBit_PositivePointSign) 0x03 else 0x02
    curve.getCurve.decodePoint(compEnc)
  }

  def apply(r: ByteString, s: ByteString, v: ByteString): ECDSASignature =
    ECDSASignature(r.toArray, s.toArray, v.toArray)

  def apply(r: Array[Byte], s: Array[Byte], v: Array[Byte]): ECDSASignature =
    ECDSASignature(new BigInteger(1, r), new BigInteger(1, s), new BigInteger(v).byteValue)
}

/**
 * ECDSASignature r and s are same as in documentation where signature is represented by tuple (r, s)
 * @param r - x coordinate of ephemeral public key modulo curve order N
 * @param s - part of the signature calculated with signer private key
 * @param v - public key recovery id
 */
final case class ECDSASignature(r: BigInteger, s: BigInteger, v: Byte)

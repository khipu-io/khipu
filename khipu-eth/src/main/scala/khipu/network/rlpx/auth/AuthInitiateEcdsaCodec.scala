package khipu.network.rlpx.auth

import akka.util.ByteString
import java.math.BigInteger
import khipu.crypto.ECDSASignature
import org.spongycastle.util.BigIntegers

trait AuthInitiateEcdsaCodec {

  def encodeECDSA(sig: ECDSASignature): ByteString = {
    import sig._

    val recoveryId: Byte = (v - 27).toByte

    ByteString(
      BigIntegers.asUnsignedByteArray(r).reverse.padTo(ECDSASignature.RLength, 0.toByte).reverse ++
        BigIntegers.asUnsignedByteArray(s).reverse.padTo(ECDSASignature.SLength, 0.toByte).reverse ++
        Array(recoveryId)
    )
  }

  def decodeECDSA(input: Array[Byte]): ECDSASignature = {
    val SIndex = 32
    val VIndex = 64

    val r = input.take(ECDSASignature.RLength)
    val s = input.slice(SIndex, SIndex + ECDSASignature.SLength)
    val v = input(VIndex) + 27
    ECDSASignature(new BigInteger(1, r), new BigInteger(1, s), v.toByte)
  }
}

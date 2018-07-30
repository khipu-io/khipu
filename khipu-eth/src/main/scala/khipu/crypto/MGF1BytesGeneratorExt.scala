package khipu.crypto

import akka.util.ByteString
import org.spongycastle.crypto.Digest

/**
 * This class is borrowed from spongycastle project
 * The only change made is addition of 'counterStart' parameter to
 * conform to Crypto++ capabilities
 */
final class MGF1BytesGeneratorExt(digest: Digest) {
  val digestSize: Int = digest.getDigestSize

  private def itoOSP(i: Int, sp: Array[Byte]) {
    sp(0) = (i >>> 24).toByte
    sp(1) = (i >>> 16).toByte
    sp(2) = (i >>> 8).toByte
    sp(3) = (i >>> 0).toByte
  }

  def generateBytes(outputLength: Int, seed: Array[Byte]): ByteString = {

    val counterStart = 1
    val hashBuf = new Array[Byte](digestSize)
    val counterValue = new Array[Byte](Integer.BYTES)

    digest.reset()

    (0 until (outputLength / digestSize + 1)).map { i =>
      itoOSP(counterStart + i, counterValue)
      digest.update(seed, 0, seed.length)
      digest.update(counterValue, 0, counterValue.length)
      digest.doFinal(hashBuf, 0)

      val spaceLeft = outputLength - (i * digestSize)

      if (spaceLeft > digestSize) {
        ByteString(hashBuf)
      } else {
        ByteString(hashBuf).dropRight(digestSize - spaceLeft)
      }
    }.reduce[ByteString] { case (a, b) => a ++ b }
  }
}

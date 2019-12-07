package khipu.crypto.hash

import org.bouncycastle.jcajce.provider.digest.BCMessageDigest
import org.bouncycastle.util.Pack

object Blake2bf {
  val MESSAGE_LENGTH_BYTES = 213

  private val IV = Array[Long](
    0x6a09e667f3bcc908L, 0xbb67ae8584caa73bL, 0x3c6ef372fe94f82bL,
    0xa54ff53a5f1d36f1L, 0x510e527fade682d1L, 0x9b05688c2b3e6c1fL,
    0x1f83d9abfb41bd6bL, 0x5be0cd19137e2179L
  )

  private val PRECOMPUTED = Array(
    Array[Byte](0, 2, 4, 6, 1, 3, 5, 7, 8, 10, 12, 14, 9, 11, 13, 15),
    Array[Byte](14, 4, 9, 13, 10, 8, 15, 6, 1, 0, 11, 5, 12, 2, 7, 3),
    Array[Byte](11, 12, 5, 15, 8, 0, 2, 13, 10, 3, 7, 9, 14, 6, 1, 4),
    Array[Byte](7, 3, 13, 11, 9, 1, 12, 14, 2, 5, 4, 15, 6, 10, 0, 8),
    Array[Byte](9, 5, 2, 10, 0, 7, 4, 15, 14, 11, 6, 3, 1, 12, 8, 13),
    Array[Byte](2, 6, 0, 8, 12, 10, 11, 3, 4, 7, 15, 1, 13, 5, 14, 9),
    Array[Byte](12, 1, 14, 4, 5, 15, 13, 10, 0, 6, 9, 8, 7, 3, 2, 11),
    Array[Byte](13, 7, 12, 3, 11, 14, 1, 9, 5, 15, 8, 2, 0, 4, 6, 10),
    Array[Byte](6, 14, 11, 0, 15, 9, 3, 8, 12, 13, 1, 10, 2, 7, 4, 5),
    Array[Byte](10, 8, 7, 1, 2, 4, 6, 5, 15, 9, 3, 13, 11, 14, 12, 0)
  )

  private val DIGEST_LENGTH = 64

  /**
   * Implementation of the `F` compression function of the Blake2b cryptographic hash function.
   *
   * <p>RFC - https://tools.ietf.org/html/rfc7693
   *
   * <p>Adapted from - https://github.com/keep-network/blake2b/blob/master/compression/f.go
   *
   * <p>Optimized for 64-bit platforms
   */
  final class Blake2bfDigest extends org.bouncycastle.crypto.Digest {

    // buffer which holds serialized input for this compression function
    // [ 4 bytes for rounds ][ 64 bytes for h ][ 128 bytes for m ]
    // [ 8 bytes for t_0 ][ 8 bytes for t_1 ][ 1 byte for f ]
    private val buffer = Array.ofDim[Byte](MESSAGE_LENGTH_BYTES)

    private var bufferPos = 0

    // deserialized inputs for f compression
    private val h = Array.ofDim[Long](8)
    private val m = Array.ofDim[Long](16)
    private val t = Array.ofDim[Long](2)
    private var f = false
    private var rounds = 12L // unsigned integer represented as long

    private val v = Array.ofDim[Long](16)

    //    // for tests
    //    Blake2bfDigest(
    //        final long[] h, final long[] m, final long[] t, final boolean f, final long rounds) {
    //      assert rounds <= 4294967295L; // uint max value
    //      buffer = new byte[MESSAGE_LENGTH_BYTES];
    //      bufferPos = 0;
    //
    //      this.h = h;
    //      this.m = m;
    //      this.t = t;
    //      this.f = f;
    //      this.rounds = rounds;
    //
    //      v = new long[16];
    //    }

    override def getAlgorithmName = "BLAKE2f"

    override def getDigestSize = DIGEST_LENGTH

    /**
     * update the message digest with a single byte.
     *
     * @param in the input byte to be entered.
     */
    override def update(in: Byte) {
      if (bufferPos == MESSAGE_LENGTH_BYTES) { // full buffer
        throw new IllegalArgumentException()
      } else {
        buffer(bufferPos) = in
        bufferPos += 1
        if (bufferPos == MESSAGE_LENGTH_BYTES) {
          initialize()
        }
      }
    }

    /**
     * update the message digest with a block of bytes.
     *
     * @param in the byte array containing the data.
     * @param offset the offset into the byte array where the data starts.
     * @param len the length of the data.
     */
    override def update(in: Array[Byte], offset: Int, len: Int) {
      if (in == null || len == 0) {
        return
      }

      if (len > MESSAGE_LENGTH_BYTES - bufferPos) {
        throw new IllegalArgumentException(
          "Attempting to update buffer with "
            + len
            + " byte(s) but there is "
            + (MESSAGE_LENGTH_BYTES - bufferPos)
            + " byte(s) left to fill"
        )
      }

      System.arraycopy(in, offset, buffer, bufferPos, len)

      bufferPos += len;

      if (bufferPos == MESSAGE_LENGTH_BYTES) {
        initialize()
      }
    }

    /**
     * close the digest, producing the final digest value. The doFinal call leaves the digest reset.
     *
     * @param out the array the digest is to be copied into.
     * @param offset the offset into the out array the digest is to start at.
     */
    override def doFinal(out: Array[Byte], offset: Int): Int = {
      if (bufferPos != 213) {
        throw new IllegalStateException("The buffer must be filled with 213 bytes")
      }

      compress()

      var i = 0
      while (i < h.length) {
        System.arraycopy(Pack.longToLittleEndian(h(i)), 0, out, i * 8, 8)
        i += 1
      }

      reset()

      return 0
    }

    /** Reset the digest back to it's initial state. */
    override def reset() {
      bufferPos = 0
      org.bouncycastle.util.Arrays.fill(buffer, 0: Byte)
      org.bouncycastle.util.Arrays.fill(h, 0: Byte)
      org.bouncycastle.util.Arrays.fill(m, 0: Byte)
      org.bouncycastle.util.Arrays.fill(t, 0: Byte)
      f = false
      rounds = 12
      org.bouncycastle.util.Arrays.fill(v, 0: Byte)
    }

    private def initialize() {
      rounds = Integer.toUnsignedLong(bytesToInt(java.util.Arrays.copyOfRange(buffer, 0, 4)))

      var i = 0
      while (i < h.length) {
        val offset = 4 + i * 8
        h(i) = bytesToLong((java.util.Arrays.copyOfRange(buffer, offset, offset + 8)))
        i += 1
      }

      i = 0
      while (i < 16) {
        val offset = 68 + i * 8
        m(i) = bytesToLong(java.util.Arrays.copyOfRange(buffer, offset, offset + 8))
        i += 1
      }

      t(0) = bytesToLong(java.util.Arrays.copyOfRange(buffer, 196, 204))
      t(1) = bytesToLong(java.util.Arrays.copyOfRange(buffer, 204, 212))

      f = buffer(212) != 0
    }

    private def bytesToInt(bytes: Array[Byte]): Int = Pack.bigEndianToInt(bytes, 0)

    private def bytesToLong(bytes: Array[Byte]): Long = Pack.littleEndianToLong(bytes, 0)

    /**
     * F is a compression function for BLAKE2b. It takes as an argument the state vector `h`,
     * message block vector `m`, offset counter `t`, final block indicator flag `f`, and number of
     * rounds `rounds`. The state vector provided as the first parameter is modified by the
     * function.
     */
    private def compress() {

      val t0 = t(0)
      val t1 = t(1)

      System.arraycopy(h, 0, v, 0, 8)
      System.arraycopy(IV, 0, v, 8, 8)

      v(12) ^= t0
      v(13) ^= t1

      if (f) {
        v(14) ^= 0xffffffffffffffffL
      }

      var j = 0L
      while (j < rounds) {
        j += 1

        val s = PRECOMPUTED((j % 10).toInt)

        mix(m(s(0)), m(s(4)), 0, 4, 8, 12)
        mix(m(s(1)), m(s(5)), 1, 5, 9, 13)
        mix(m(s(2)), m(s(6)), 2, 6, 10, 14)
        mix(m(s(3)), m(s(7)), 3, 7, 11, 15)
        mix(m(s(8)), m(s(12)), 0, 5, 10, 15)
        mix(m(s(9)), m(s(13)), 1, 6, 11, 12)
        mix(m(s(10)), m(s(14)), 2, 7, 8, 13)
        mix(m(s(11)), m(s(15)), 3, 4, 9, 14)
      }

      // update h:
      var offset = 0
      while (offset < h.length) {
        h(offset) ^= v(offset) ^ v(offset + 8)
        offset += 1
      }
    }

    private def mix(a: Long, b: Long, i: Int, j: Int, k: Int, l: Int) {
      v(i) += a + v(j)
      v(l) = java.lang.Long.rotateLeft(v(l) ^ v(i), -32)
      v(k) += v(l)
      v(j) = java.lang.Long.rotateLeft(v(j) ^ v(k), -24)

      v(i) += b + v(j)
      v(l) = java.lang.Long.rotateLeft(v(l) ^ v(i), -16)
      v(k) += v(l)
      v(j) = java.lang.Long.rotateLeft(v(j) ^ v(k), -63)
    }
  }
}
final class Blake2bf extends BCMessageDigest(new Blake2bf.Blake2bfDigest())
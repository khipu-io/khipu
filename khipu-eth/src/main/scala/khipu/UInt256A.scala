package khipu

import akka.util.ByteString
import khipu.util.BytesUtil
import java.math.BigInteger

object UInt256_biginteger {
  object UInt256 {

    val SIZE = 32 // size in bytes
    val SIZE_IN_BITS = 256 // 32 * 8

    private val ZERO = BigInteger.ZERO
    private val ONE = BigInteger.ONE
    private val TEN = BigInteger.TEN
    private val TWO = BigInteger.valueOf(2)
    private val MAX_INT = BigInteger.valueOf(Int.MaxValue)
    private val MAX_LONG = BigInteger.valueOf(Long.MaxValue)
    private val THIRTY_ONE = BigInteger.valueOf(31)
    private val THIRTY_TWO = BigInteger.valueOf(32)
    private val TWO_FIVE_SIX = BigInteger.valueOf(256)

    // --- Beware mutable MODULUS/MAX_VALUE/MAX_SIGNED_VALUE, use them with copy
    private val MODULUS = TWO.pow(SIZE_IN_BITS)
    private val MAX_VALUE = TWO.pow(SIZE_IN_BITS) subtract BigInteger.ONE
    private val MAX_SIGNED_VALUE = TWO.pow(SIZE_IN_BITS - 1) subtract BigInteger.ONE

    // UInt256 value should be put behind MODULUS (will be used in UInt256 constructor) etc

    val Modulus = safe(TWO.pow(SIZE_IN_BITS))

    val Zero: UInt256 = safe(ZERO)
    val One: UInt256 = safe(ONE)
    val Two: UInt256 = safe(TWO)
    val Ten: UInt256 = safe(TEN)
    val MaxInt: UInt256 = safe(MAX_INT)
    val MaxLong: UInt256 = safe(MAX_LONG)
    val ThirtyTwo: UInt256 = safe(THIRTY_TWO)
    val TwoFiveSix = safe(TWO_FIVE_SIX)

    def apply(n: Int): UInt256 = safe(n)
    def apply(n: Long): UInt256 = safe(n)
    def apply(b: Boolean): UInt256 = if (b) One else Zero
    def apply(bytes: ByteString): UInt256 = apply(bytes.toArray)
    def apply(bytes: Hash): UInt256 = apply(bytes.bytes)

    // with bound limited
    def apply(n: BigInteger): UInt256 = new UInt256(boundBigInt(n))
    def apply(bigEndianBytes: Array[Byte]): UInt256 = {
      require(bigEndianBytes.length <= SIZE, s"Input byte array cannot be longer than $SIZE: ${bigEndianBytes.length}")
      if (bigEndianBytes.length == 0) {
        UInt256.Zero
      } else {
        safe(bigEndianBytes)
      }
    }

    def safe(n: Int): UInt256 = new UInt256(BigInteger.valueOf(n))
    def safe(n: Long): UInt256 = new UInt256(BigInteger.valueOf(n))
    def safe(n: BigInteger): UInt256 = new UInt256(n)
    def safe(bigEndianMag: Array[Byte]): UInt256 = new UInt256(new BigInteger(1, bigEndianMag))

    private def boundBigInt(n: BigInteger): BigInteger = {
      if (n.signum > 0) {
        if (n.compareTo(MODULUS) >= 0) {
          n remainder MODULUS
        } else {
          n
        }
      } else if (n.signum < 0) {
        val r = n remainder MODULUS
        if (r.signum == 0) {
          r
        } else {
          r add MODULUS
        }
      } else {
        ZERO
      }
    }

    /**
     * Number of 32-byte UInt256s required to hold n bytes (~= math.ceil(n / 32))
     * We assume n is not neseccary to exceed Long.MaxValue, and use Long here
     */
    def wordsForBytes(n: Long): Long = if (n == 0) 0 else (n - 1) / SIZE + 1
  }

  /**
   * Represents 256 bit unsigned integers with standard arithmetic, byte-wise operation and EVM-specific extensions
   * require(n.signum >= 0 && n.compareTo(MODULUS) < 0, s"Invalid UInt256 value: $n") --- already checked in apply(n: BigInteger)
   */
  final class UInt256 private (val n: BigInteger) extends Ordered[UInt256] {
    import UInt256._

    // EVM-specific arithmetic
    private lazy val signed = if (n.compareTo(MAX_SIGNED_VALUE) > 0) (n subtract MODULUS) else n

    lazy val bigEndianMag = n.toByteArray

    lazy val nonZeroLeadingBytes: Array[Byte] = {
      val src = bigEndianMag
      var i = 0
      while (i < src.length && src(i) == 0) {
        i += 1
      }
      if (i == 0) {
        src
      } else {
        val dest = Array.ofDim[Byte](src.length - i)
        System.arraycopy(src, i, dest, 0, dest.length)
        dest
      }
    }

    /**
     * Converts an UInt256 to an Array[Byte].
     * Output Array[Byte] is padded with zeros from the left side up to UInt256.Size bytes.
     */
    lazy val bytes: Array[Byte] = {
      val src = bigEndianMag
      if (src.length == SIZE) {
        src
      } else {
        val dst = Array.ofDim[Byte](SIZE) // filled by 0 by default
        val len = math.min(src.length, SIZE)
        val srcOffset = math.max(0, src.length - len)
        val dstOffset = dst.length - len
        System.arraycopy(src, srcOffset, dst, dstOffset, len)
        dst
      }
    }

    /**
     * Used for gas calculation for EXP opcode. See YP Appendix H.1 (220)
     * For n > 0: (n.bitLength - 1) / 8 + 1 == 1 + floor(log_256(n))
     *
     * @return Size in bytes excluding the leading 0 bytes
     */
    def byteSize: Int = {
      if (n.signum == 0) {
        0
      } else {
        (n.bitLength - 1) / 8 + 1
      }
    }

    def getByte(that: UInt256): UInt256 = {
      if (that.n.compareTo(THIRTY_ONE) > 0) {
        Zero
      } else {
        UInt256.safe(bytes(that.n.intValue).toInt & 0xff)
      }
    }

    // standard arithmetic (note the use of new instead of apply where result is guaranteed to be within bounds)
    def &(that: UInt256): UInt256 = UInt256.safe(n and that.n)
    def |(that: UInt256): UInt256 = UInt256.safe(n or that.n)
    def ^(that: UInt256): UInt256 = UInt256.safe(n xor that.n)
    def unary_- : UInt256 = UInt256(n.negate)
    def unary_~ : UInt256 = UInt256(n.not)
    def +(that: UInt256): UInt256 = UInt256(n add that.n)
    def -(that: UInt256): UInt256 = UInt256(n subtract that.n)
    def *(that: UInt256): UInt256 = UInt256(n multiply that.n)
    def /(that: UInt256): UInt256 = UInt256.safe(n divide that.n)
    def **(that: UInt256): UInt256 = UInt256.safe(n.modPow(that.n, MODULUS))

    def +(that: Int): UInt256 = UInt256(n add BigInteger.valueOf(that))
    def -(that: Int): UInt256 = UInt256(n subtract BigInteger.valueOf(that))
    def *(that: Int): UInt256 = UInt256(n multiply BigInteger.valueOf(that))
    def /(that: Int): UInt256 = UInt256.safe(n divide BigInteger.valueOf(that))

    def +(that: Long): UInt256 = UInt256(n add BigInteger.valueOf(that))
    def -(that: Long): UInt256 = UInt256(n subtract BigInteger.valueOf(that))
    def *(that: Long): UInt256 = UInt256(n multiply BigInteger.valueOf(that))
    def /(that: Long): UInt256 = UInt256.safe(n divide BigInteger.valueOf(that))

    def pow(that: Int): UInt256 = UInt256(n pow that)

    def min(that: UInt256): UInt256 = if (n.compareTo(that.n) < 0) this else that
    def max(that: UInt256): UInt256 = if (n.compareTo(that.n) > 0) this else that
    def isZero: Boolean = n.signum == 0
    def nonZero: Boolean = n.signum != 0

    def div(that: UInt256): UInt256 = if (that.n.signum == 0) Zero else UInt256.safe(n divide that.n)
    def sdiv(that: UInt256): UInt256 = if (that.n.signum == 0) Zero else UInt256(signed divide that.signed)
    def mod(that: UInt256): UInt256 = if (that.n.signum == 0) Zero else UInt256(n mod that.n)
    def smod(that: UInt256): UInt256 = if (that.n.signum == 0) Zero else UInt256(signed remainder that.signed.abs)
    def addmod(that: UInt256, modulus: UInt256): UInt256 = if (modulus.n.signum == 0) Zero else UInt256.safe((n add that.n) remainder modulus.n)
    def mulmod(that: UInt256, modulus: UInt256): UInt256 = if (modulus.n.signum == 0) Zero else UInt256.safe((n multiply that.n) mod modulus.n)

    def slt(that: UInt256): Boolean = signed.compareTo(that.signed) < 0
    def sgt(that: UInt256): Boolean = signed.compareTo(that.signed) > 0

    def signExtend(that: UInt256): UInt256 = {
      if (that.n.signum < 0 || that.n.compareTo(THIRTY_ONE) > 0) {
        this
      } else {
        val idx = that.n.byteValue
        val negative = n testBit (idx * 8 + 7)
        val mask = (ONE shiftLeft ((idx + 1) * 8)) subtract ONE
        val newN = if (negative) {
          n or (MAX_VALUE xor mask)
        } else {
          n and mask
        }
        UInt256.safe(newN)
      }
    }

    def compare(that: UInt256): Int = n.compareTo(that.n)

    override def equals(that: Any): Boolean = {
      that match {
        case that: UInt256    => n == that.n
        case that: BigInteger => n == that
        case that: Byte       => n == BigInteger.valueOf(that)
        case that: Short      => n == BigInteger.valueOf(that)
        case that: Int        => n == BigInteger.valueOf(that)
        case that: Long       => n == BigInteger.valueOf(that)
        case other            => other == n
      }
    }

    override def hashCode: Int = n.hashCode
    override def toString: String = toSignedDecString

    def toDecString: String = n.toString
    def toSignedDecString: String = signed.toString
    def toHexString: String = {
      val hex = f"${n}%x"
      // add zero if odd number of digits
      val extraZero = if (hex.length % 2 == 0) "" else "0"
      s"0x$extraZero$hex"
    }

    /**
     * @return an Int with MSB=0, thus a value in range [0, Int.MaxValue]
     */
    def toInt: Int = n.intValue & Int.MaxValue

    /**
     * @return a Long with MSB=0, thus a value in range [0, Long.MaxValue]
     */
    def toLong: Long = n.longValue & Long.MaxValue

    def toMaxInt: Int = if (n.compareTo(MAX_INT) <= 0) n.intValue & Int.MaxValue else Int.MaxValue
    def toMaxLong: Long = if (n.compareTo(MAX_LONG) <= 0) n.longValue & Long.MaxValue else Long.MaxValue

    // return a minimum negtive when value not in range. TODO a safe UInt and ULong for gas/memory calculation
    def toUInt(): Int = if (n.compareTo(MAX_INT) <= 0) n.intValue & Int.MaxValue else Int.MinValue
    def toULong(): Long = if (n.compareTo(MAX_LONG) <= 0) n.longValue & Long.MaxValue else Long.MinValue

    def bytesOccupied: Int = {
      val firstNonZero = BytesUtil.firstNonZeroByte(bytes)
      if (firstNonZero == -1) {
        0
      } else {
        31 - firstNonZero + 1
      }
    }

    /**
     * Converts this to an int, checking for lost information.
     * If this is out of the possible range for an int result
     * then an ArithmeticException is thrown.
     *
     * @return this converted to an int.
     * @throws ArithmeticException - if this will not fit in an int. // TODO
     */
    def intValue: Int = {
      var intVal = 0
      var i = 0
      while (i < bytes.length) {
        intVal = (intVal << 8) + (bytes(i) & 0xff)
        i += 1
      }
      intVal
    }

    /**
     * In case of int overflow returns Integer.MAX_VALUE
     * otherwise works as #intValue()
     */
    def intValueSafe: Int = {
      if (bytesOccupied > 4) {
        Int.MaxValue
      } else {
        val intVal = intValue
        if (intVal < 0) {
          Int.MaxValue
        } else {
          intVal
        }
      }
    }

    /**
     * Converts this to a long, checking for lost information.
     * If this is out of the possible range for a long result
     * then an ArithmeticException is thrown.
     *
     * @return this converted to a long.
     * @throws ArithmeticException - if this will not fit in a long. // TODO
     */
    def longValue: Long = {
      var longVal = 0
      var i = 0
      while (i < bytes.length) {
        longVal = (longVal << 8) + (bytes(i) & 0xff)
        i += 1
      }
      longVal
    }

    /**
     * In case of long overflow returns Long.MAX_VALUE
     * otherwise works as #longValue()
     */
    def longValueSafe: Long = {
      if (bytesOccupied > 8) {
        Long.MaxValue
      } else {
        val longVal = longValue
        if (longVal < 0) {
          Long.MaxValue
        } else {
          longVal
        }
      }
    }
  }
}
package khipu

import akka.util.ByteString
import java.math.BigInteger
import khipu.util.BytesUtil

object DataWord_bigint {
  object DataWord {

    val SIZE = 32 // size in bytes
    val SIZE_IN_BITS = 256 // 32 * 8

    private val ZERO = new BigInt(0)
    private val ONE = new BigInt(1)
    private val TEN = new BigInt(10)
    private val TWO = new BigInt(2)
    private val MAX_INT = new BigInt(Int.MaxValue)
    private val MAX_LONG = new BigInt(Long.MaxValue)
    private val THIRTY_ONE = new BigInt(31)
    private val THIRTY_TWO = new BigInt(32)
    private val TWO_FIVE_SIX = new BigInt(256)

    // --- Beware mutable MODULUS/MAX_VALUE/MAX_SIGNED_VALUE, use them with copy
    private val MODULUS = TWO.copy.j_pow(SIZE_IN_BITS)
    private val MAX_VALUE = TWO.copy.j_pow(SIZE_IN_BITS) j_subtract ONE.copy
    private val MAX_SIGNED_VALUE = TWO.copy.j_pow(SIZE_IN_BITS - 1) j_subtract ONE.copy

    // DataWord value should be put behind MODULUS (will be used in DataWord constructor) etc

    def Modulus = safe(MODULUS.copy)

    def Zero: DataWord = safe(ZERO.copy)
    def One: DataWord = safe(ONE.copy)
    def Two: DataWord = safe(TWO.copy)
    def Ten: DataWord = safe(TEN.copy)
    def MaxInt: DataWord = safe(MAX_INT.copy)
    def MaxLong: DataWord = safe(MAX_LONG.copy)
    def ThirtyTwo: DataWord = safe(THIRTY_TWO.copy)
    def TwoFiveSix = safe(TWO_FIVE_SIX.copy)

    def apply(n: Int): DataWord = safe(n)
    def apply(n: Long): DataWord = safe(n)
    def apply(b: Boolean): DataWord = if (b) safe(ONE.copy) else safe(ZERO.copy)
    def apply(bytes: ByteString): DataWord = apply(bytes.toArray)
    def apply(bytes: Hash): DataWord = apply(bytes.bytes)

    // with bound limited
    def apply(n: BigInt): DataWord = new DataWord(boundBigInt(n))
    def apply(n: BigInteger): DataWord = new DataWord(boundBigInt(new BigInt(n)))
    def apply(bigEndianBytes: Array[Byte]): DataWord = {
      require(bigEndianBytes.length <= SIZE, s"Input byte array cannot be longer than $SIZE: ${bigEndianBytes.length}")
      if (bigEndianBytes.length == 0) {
        DataWord.Zero
      } else {
        safe(bigEndianBytes)
      }
    }

    def safe(n: Int): DataWord = new DataWord(new BigInt(n))
    def safe(n: Long): DataWord = new DataWord(new BigInt(n))
    def safe(n: BigInt): DataWord = new DataWord(n)
    def safe(n: BigInteger): DataWord = new DataWord(new BigInt(n))
    def safe(bigEndianBytes: Array[Byte]): DataWord = new DataWord(new BigInt(new BigInteger(1, bigEndianBytes)))

    private def boundBigInt(n: BigInt): BigInt = {
      if (n.isPositive) {
        if (n.compareTo(MODULUS) >= 0) {
          n.copy j_remainder MODULUS.copy
        } else {
          n
        }
      } else if (n.isNegative) {
        val r = n.copy j_remainder MODULUS.copy
        if (r.isZero) {
          r
        } else {
          r j_add MODULUS.copy
        }
      } else {
        ZERO.copy
      }
    }

    /**
     * Number of 32-byte DataWords required to hold n bytes (~= math.ceil(n / 32))
     * We assume n is not neseccary to exceed Long.MaxValue, and use Long here
     */
    def wordsForBytes(n: Long): Long = if (n == 0) 0 else (n - 1) / SIZE + 1
  }

  /**
   * Represents 256 bit unsigned integers with standard arithmetic, byte-wise operation and EVM-specific extensions
   * require(n.signum >= 0 && n.compareTo(MODULUS) < 0, s"Invalid DataWord value: $n") --- already checked in apply(n: BigInteger)
   */
  final class DataWord private (val n: BigInt) extends Ordered[DataWord] {
    import DataWord._

    // ==== NOTE: n is mutable

    // EVM-specific arithmetic
    private lazy val signed = if (n.compareTo(MAX_SIGNED_VALUE) > 0) (n.copy j_subtract MODULUS.copy) else n

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
     * Converts an DataWord to an Array[Byte].
     * Output Array[Byte] is padded with zeros from the left side up to DataWord.Size bytes.
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
      if (n.isZero) {
        0
      } else {
        (n.bitLength - 1) / 8 + 1
      }
    }

    def getByte(that: DataWord): DataWord = {
      if (that.n.compareTo(THIRTY_ONE.copy) > 0) {
        Zero
      } else {
        DataWord.safe(bytes(that.n.intValue).toInt & 0xff)
      }
    }

    // standard arithmetic (note the use of new instead of apply where result is guaranteed to be within bounds)
    def &(that: DataWord): DataWord = DataWord.safe(n.copy j_and that.n.copy)
    def |(that: DataWord): DataWord = DataWord.safe(n.copy j_or that.n.copy)
    def ^(that: DataWord): DataWord = DataWord.safe(n.copy j_xor that.n.copy)
    def unary_- : DataWord = DataWord(n.copy.negate)
    def unary_~ : DataWord = DataWord(n.copy.j_not)
    def +(that: DataWord): DataWord = DataWord(n.copy j_add that.n.copy)
    def -(that: DataWord): DataWord = DataWord(n.copy j_subtract that.n.copy)
    def *(that: DataWord): DataWord = DataWord(n.copy j_multiply that.n.copy)
    def /(that: DataWord): DataWord = DataWord.safe(n.copy j_divide that.n.copy)
    def **(that: DataWord): DataWord = DataWord.safe(n.copy.j_modPow(that.n.copy, MODULUS.copy))

    def +(that: Int): DataWord = DataWord(n.copy j_add new BigInt(that))
    def -(that: Int): DataWord = DataWord(n.copy j_subtract new BigInt(that))
    def *(that: Int): DataWord = DataWord(n.copy j_multiply new BigInt(that))
    def /(that: Int): DataWord = DataWord.safe(n.copy j_divide new BigInt(that))

    def +(that: Long): DataWord = DataWord(n.copy j_add new BigInt(that))
    def -(that: Long): DataWord = DataWord(n.copy j_subtract new BigInt(that))
    def *(that: Long): DataWord = DataWord(n.copy j_multiply new BigInt(that))
    def /(that: Long): DataWord = DataWord.safe(n.copy j_divide new BigInt(that))

    def pow(that: Int): DataWord = DataWord(n.copy j_pow that)

    def min(that: DataWord): DataWord = if (n.compareTo(that.n) < 0) this else that
    def max(that: DataWord): DataWord = if (n.compareTo(that.n) > 0) this else that
    def isZero: Boolean = n.isZero
    def nonZero: Boolean = !n.isZero

    def div(that: DataWord): DataWord = if (that.n.isZero) Zero else new DataWord(n.copy j_divide that.n.copy)
    def sdiv(that: DataWord): DataWord = if (that.n.isZero) Zero else DataWord(signed.copy j_divide that.signed.copy)
    def mod(that: DataWord): DataWord = if (that.n.isZero) Zero else DataWord(n.copy j_mod that.n.copy)
    def smod(that: DataWord): DataWord = if (that.n.isZero) Zero else DataWord(signed.copy j_remainder that.signed.copy.abs)
    def addmod(that: DataWord, modulus: DataWord): DataWord = if (modulus.n.isZero) Zero else DataWord.safe((n.copy j_add that.n.copy) j_remainder modulus.n.copy)
    def mulmod(that: DataWord, modulus: DataWord): DataWord = if (modulus.n.isZero) Zero else DataWord.safe((n.copy j_multiply that.n.copy) j_mod modulus.n.copy)

    def slt(that: DataWord): Boolean = signed.compareTo(that.signed) < 0
    def sgt(that: DataWord): Boolean = signed.compareTo(that.signed) > 0

    def signExtend(that: DataWord): DataWord = {
      if (that.n.isNegative || that.n.compareTo(THIRTY_ONE.copy) > 0) {
        this
      } else {
        val idx = that.n.byteValue
        val negative = n testBit (idx * 8 + 7)
        val mask = (ONE.copy j_shiftLeft ((idx + 1) * 8)) j_subtract ONE.copy
        val newN = if (negative) {
          n.copy j_or (MAX_VALUE.copy j_xor mask.copy)
        } else {
          n.copy j_and mask.copy
        }
        DataWord.safe(newN)
      }
    }

    def compare(that: DataWord): Int = n.compareTo(that.n)

    override def equals(that: Any): Boolean = {
      that match {
        case that: DataWord => (this eq that) || this.n == that.n
        case that: BigInt   => this.n == that
        case that: Byte     => this.n == that
        case that: Short    => this.n == that
        case that: Int      => this.n == that
        case that: Long     => this.n == that
        case _              => false
      }
    }

    override def hashCode: Int = n.hashCode
    override def toString: String = toSignedDecString

    def toDecString: String = n.toString
    def toSignedDecString: String = signed.toString
    def toHexString: String = {
      val hex = f"${n.toBigInteger}%x"
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

    def toMaxInt: Int = if (n.compareTo(MAX_INT.copy) <= 0) n.intValue & Int.MaxValue else Int.MaxValue
    def toMaxLong: Long = if (n.compareTo(MAX_LONG.copy) <= 0) n.longValue & Long.MaxValue else Long.MaxValue

    // return a minimum negtive when value not in range. TODO a safe UInt and ULong for gas/memory calculation
    def toUInt(): Int = if (n.compareTo(MAX_INT.copy) <= 0) n.intValue & Int.MaxValue else Int.MinValue
    def toULong(): Long = if (n.compareTo(MAX_LONG.copy) <= 0) n.longValue & Long.MaxValue else Long.MinValue

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
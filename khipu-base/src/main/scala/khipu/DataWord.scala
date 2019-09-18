package khipu

import akka.util.ByteString
import khipu.util.BytesUtil
import java.math.BigInteger

object DataWord {

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
  private val MAX_POW = TWO_FIVE_SIX

  // --- Beware mutable MODULUS/MAX_VALUE/MAX_SIGNED_VALUE, use them with copy
  val MODULUS = TWO.pow(SIZE_IN_BITS) // or ONE.shiftLeft(SIZE_IN_BITS)
  val MAX_VALUE = TWO.pow(SIZE_IN_BITS) subtract BigInteger.ONE
  val MAX_SIGNED_VALUE = TWO.pow(SIZE_IN_BITS - 1) subtract BigInteger.ONE

  // DataWord value should be put behind MODULUS (will be used in DataWord constructor) etc

  val Modulus = safe(TWO.pow(SIZE_IN_BITS))

  val Zero: DataWord = safe(ZERO)
  val One: DataWord = safe(ONE)
  val Two: DataWord = safe(TWO)
  val Ten: DataWord = safe(TEN)
  val MaxInt: DataWord = safe(MAX_INT)
  val MaxLong: DataWord = safe(MAX_LONG)
  val ThirtyTwo: DataWord = safe(THIRTY_TWO)
  val TwoFiveSix = safe(TWO_FIVE_SIX)

  def apply(n: Int): DataWord = safe(n)
  def apply(n: Long): DataWord = safe(n)
  def apply(b: Boolean): DataWord = if (b) One else Zero
  def apply(bytes: ByteString): DataWord = apply(bytes.toArray)
  def apply(bytes: Hash): DataWord = apply(bytes.bytes)

  // with bound limited
  def apply(n: BigInteger): DataWord = new DataWord(boundBigInt(n))
  def apply(bigEndianBytes: Array[Byte]): DataWord = {
    require(bigEndianBytes.length <= SIZE, s"Input byte array cannot be longer than $SIZE: ${bigEndianBytes.length}")
    if (bigEndianBytes.length == 0) {
      DataWord.Zero
    } else {
      safe(bigEndianBytes)
    }
  }

  def safe(n: Int): DataWord = new DataWord(BigInteger.valueOf(n))
  def safe(n: Long): DataWord = new DataWord(BigInteger.valueOf(n))
  def safe(n: BigInteger): DataWord = new DataWord(n)
  def safe(bigEndianMag: Array[Byte]): DataWord = new DataWord(new BigInteger(1, bigEndianMag))

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
   * Number of 32-byte DataWords required to hold n bytes (~= math.ceil(n / 32))
   * We assume n is not neseccary to exceed Long.MaxValue, and use Long here
   */
  def wordsForBytes(n: Long): Long = if (n == 0) 0 else (n - 1) / SIZE + 1

  // --- simple test
  def main(args: Array[String]) {
    var a1 = DataWord(hexDecode("8000000000000000000000000000000000000000000000000000000000000000"))
    var a2 = DataWord(hexDecode("01"))
    var r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xc000000000000000000000000000000000000000000000000000000000000000

    a1 = DataWord(hexDecode("8000000000000000000000000000000000000000000000000000000000000000"))
    a2 = DataWord(hexDecode("ff"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = DataWord(hexDecode("8000000000000000000000000000000000000000000000000000000000000000"))
    a2 = DataWord(hexDecode("0100"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = DataWord(hexDecode("8000000000000000000000000000000000000000000000000000000000000000"))
    a2 = DataWord(hexDecode("0101"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = DataWord(hexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = DataWord(hexDecode("00"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = DataWord(hexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = DataWord(hexDecode("01"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = DataWord(hexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = DataWord(hexDecode("ff"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = DataWord(hexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = DataWord(hexDecode("0100"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = DataWord(hexDecode("0000000000000000000000000000000000000000000000000000000000000000"))
    a2 = DataWord(hexDecode("01"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000000

    a1 = DataWord(hexDecode("4000000000000000000000000000000000000000000000000000000000000000"))
    a2 = DataWord(hexDecode("fe"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000001

    a1 = DataWord(hexDecode("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = DataWord(hexDecode("f8"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x000000000000000000000000000000000000000000000000000000000000007f

    a1 = DataWord(hexDecode("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = DataWord(hexDecode("fe"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000001

    a1 = DataWord(hexDecode("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = DataWord(hexDecode("ff"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000000

    a1 = DataWord(hexDecode("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = DataWord(hexDecode("0100"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000000
  }

}
/**
 * Natural unit of data used by the VM design.
 * Represents 256 bit unsigned integers with standard arithmetic, byte-wise operation and EVM-specific extensions
 * require(n.signum >= 0 && n.compareTo(MODULUS) < 0, s"Invalid DataWord value: $n") --- already checked in apply(n: BigInteger)
 */
final class DataWord private (val n: BigInteger) extends Ordered[DataWord] {
  import DataWord._

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
    if (n.signum == 0) {
      0
    } else {
      (n.bitLength - 1) / 8 + 1
    }
  }

  def getByte(that: DataWord): DataWord = {
    if (that.n.compareTo(THIRTY_ONE) > 0) {
      Zero
    } else {
      DataWord.safe(bytes(that.n.intValue).toInt & 0xff)
    }
  }

  // standard arithmetic (note the use of new instead of apply where result is guaranteed to be within bounds)
  def &(that: DataWord): DataWord = DataWord.safe(n and that.n)
  def |(that: DataWord): DataWord = DataWord.safe(n or that.n)
  def ^(that: DataWord): DataWord = DataWord.safe(n xor that.n)
  def unary_- : DataWord = DataWord(n.negate)
  def unary_~ : DataWord = DataWord(n.not)
  def +(that: DataWord): DataWord = DataWord(n add that.n)
  def -(that: DataWord): DataWord = DataWord(n subtract that.n)
  def *(that: DataWord): DataWord = DataWord(n multiply that.n)
  def /(that: DataWord): DataWord = DataWord.safe(n divide that.n)
  def **(that: DataWord): DataWord = DataWord.safe(n.modPow(that.n, MODULUS))

  def +(that: Int): DataWord = DataWord(n add BigInteger.valueOf(that))
  def -(that: Int): DataWord = DataWord(n subtract BigInteger.valueOf(that))
  def *(that: Int): DataWord = DataWord(n multiply BigInteger.valueOf(that))
  def /(that: Int): DataWord = DataWord.safe(n divide BigInteger.valueOf(that))

  def +(that: Long): DataWord = DataWord(n add BigInteger.valueOf(that))
  def -(that: Long): DataWord = DataWord(n subtract BigInteger.valueOf(that))
  def *(that: Long): DataWord = DataWord(n multiply BigInteger.valueOf(that))
  def /(that: Long): DataWord = DataWord.safe(n divide BigInteger.valueOf(that))

  def pow(that: Int): DataWord = DataWord(n pow that)

  def min(that: DataWord): DataWord = if (n.compareTo(that.n) < 0) this else that
  def max(that: DataWord): DataWord = if (n.compareTo(that.n) > 0) this else that
  def isZero: Boolean = n.signum == 0
  def nonZero: Boolean = n.signum != 0

  def div(that: DataWord): DataWord = if (that.n.signum == 0) Zero else DataWord.safe(n divide that.n)
  def sdiv(that: DataWord): DataWord = if (that.n.signum == 0) Zero else DataWord(signed divide that.signed)
  def mod(that: DataWord): DataWord = if (that.n.signum == 0) Zero else DataWord(n mod that.n)
  def smod(that: DataWord): DataWord = if (that.n.signum == 0) Zero else DataWord(signed remainder that.signed.abs)
  def addmod(that: DataWord, modulus: DataWord): DataWord = if (modulus.n.signum == 0) Zero else DataWord.safe((n add that.n) remainder modulus.n)
  def mulmod(that: DataWord, modulus: DataWord): DataWord = if (modulus.n.signum == 0) Zero else DataWord.safe((n multiply that.n) mod modulus.n)

  def slt(that: DataWord): Boolean = signed.compareTo(that.signed) < 0
  def sgt(that: DataWord): Boolean = signed.compareTo(that.signed) > 0

  def signExtend(that: DataWord): DataWord = {
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
      DataWord.safe(newN)
    }
  }

  /**
   * Shift left, both this and input arg are treated as unsigned
   * @param arg
   * @return this << arg
   */
  def shiftLeft(arg: DataWord): DataWord = {
    if (arg.n.compareTo(MAX_POW) >= 0) {
      Zero
    } else {
      DataWord(n.shiftLeft(arg.intValueSafe))
    }
  }

  /**
   * Shift right, both this and input arg are treated as unsigned
   * @param arg
   * @return this >> arg
   */
  def shiftRight(arg: DataWord): DataWord = {
    if (arg.n.compareTo(MAX_POW) >= 0) {
      Zero
    } else {
      DataWord(n.shiftRight(arg.intValueSafe))
    }
  }

  /**
   * Shift right, this is signed, while input arg is treated as unsigned
   * @param arg
   * @return this >> arg
   */
  def shiftRightSigned(arg: DataWord): DataWord = {
    if (arg.n.compareTo(MAX_POW) >= 0) {
      if (isNegative) {
        DataWord(BigInteger.ONE.negate)
      } else {
        Zero
      }
    } else {

      DataWord(signed.shiftRight(arg.intValueSafe))
    }
  }

  /**
   * only in case of signed operation when the number is explicit defined
   * as negative
   */
  private def isNegative = {
    signed.signum < 0
  }

  def compare(that: DataWord): Int = n.compareTo(that.n)

  override def equals(any: Any): Boolean = {
    any match {
      case that: DataWord   => (this eq that) || n == that.n
      case that: BigInteger => n == that
      case that: Byte       => n == BigInteger.valueOf(that)
      case that: Short      => n == BigInteger.valueOf(that)
      case that: Int        => n == BigInteger.valueOf(that)
      case that: Long       => n == BigInteger.valueOf(that)
      case _                => false
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
package khipu

import akka.util.ByteString
import khipu.util.BytesUtil
import java.math.BigInteger

object EvmWord {

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
  private val MODULUS = TWO.pow(SIZE_IN_BITS)
  private val MAX_VALUE = TWO.pow(SIZE_IN_BITS) subtract BigInteger.ONE
  private val MAX_SIGNED_VALUE = TWO.pow(SIZE_IN_BITS - 1) subtract BigInteger.ONE

  // EvmWord value should be put behind MODULUS (will be used in EvmWord constructor) etc

  val Modulus = safe(TWO.pow(SIZE_IN_BITS))

  val Zero: EvmWord = safe(ZERO)
  val One: EvmWord = safe(ONE)
  val Two: EvmWord = safe(TWO)
  val Ten: EvmWord = safe(TEN)
  val MaxInt: EvmWord = safe(MAX_INT)
  val MaxLong: EvmWord = safe(MAX_LONG)
  val ThirtyTwo: EvmWord = safe(THIRTY_TWO)
  val TwoFiveSix = safe(TWO_FIVE_SIX)

  def apply(n: Int): EvmWord = safe(n)
  def apply(n: Long): EvmWord = safe(n)
  def apply(b: Boolean): EvmWord = if (b) One else Zero
  def apply(bytes: ByteString): EvmWord = apply(bytes.toArray)
  def apply(bytes: Hash): EvmWord = apply(bytes.bytes)

  // with bound limited
  def apply(n: BigInteger): EvmWord = new EvmWord(boundBigInt(n))
  def apply(bigEndianBytes: Array[Byte]): EvmWord = {
    require(bigEndianBytes.length <= SIZE, s"Input byte array cannot be longer than $SIZE: ${bigEndianBytes.length}")
    if (bigEndianBytes.length == 0) {
      EvmWord.Zero
    } else {
      safe(bigEndianBytes)
    }
  }

  def safe(n: Int): EvmWord = new EvmWord(BigInteger.valueOf(n))
  def safe(n: Long): EvmWord = new EvmWord(BigInteger.valueOf(n))
  def safe(n: BigInteger): EvmWord = new EvmWord(n)
  def safe(bigEndianMag: Array[Byte]): EvmWord = new EvmWord(new BigInteger(1, bigEndianMag))

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
   * Number of 32-byte EvmWords required to hold n bytes (~= math.ceil(n / 32))
   * We assume n is not neseccary to exceed Long.MaxValue, and use Long here
   */
  def wordsForBytes(n: Long): Long = if (n == 0) 0 else (n - 1) / SIZE + 1

  // --- simple test
  def main(args: Array[String]) {
    var a1 = EvmWord(hexDecode("8000000000000000000000000000000000000000000000000000000000000000"))
    var a2 = EvmWord(hexDecode("01"))
    var r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xc000000000000000000000000000000000000000000000000000000000000000

    a1 = EvmWord(hexDecode("8000000000000000000000000000000000000000000000000000000000000000"))
    a2 = EvmWord(hexDecode("ff"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = EvmWord(hexDecode("8000000000000000000000000000000000000000000000000000000000000000"))
    a2 = EvmWord(hexDecode("0100"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = EvmWord(hexDecode("8000000000000000000000000000000000000000000000000000000000000000"))
    a2 = EvmWord(hexDecode("0101"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = EvmWord(hexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = EvmWord(hexDecode("00"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = EvmWord(hexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = EvmWord(hexDecode("01"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = EvmWord(hexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = EvmWord(hexDecode("ff"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = EvmWord(hexDecode("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = EvmWord(hexDecode("0100"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff

    a1 = EvmWord(hexDecode("0000000000000000000000000000000000000000000000000000000000000000"))
    a2 = EvmWord(hexDecode("01"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000000

    a1 = EvmWord(hexDecode("4000000000000000000000000000000000000000000000000000000000000000"))
    a2 = EvmWord(hexDecode("fe"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000001

    a1 = EvmWord(hexDecode("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = EvmWord(hexDecode("f8"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x000000000000000000000000000000000000000000000000000000000000007f

    a1 = EvmWord(hexDecode("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = EvmWord(hexDecode("fe"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000001

    a1 = EvmWord(hexDecode("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = EvmWord(hexDecode("ff"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000000

    a1 = EvmWord(hexDecode("7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
    a2 = EvmWord(hexDecode("0100"))
    r = a1.shiftRightSigned(a2)
    println(r.toHexString) // 0x0000000000000000000000000000000000000000000000000000000000000000
  }

}
/**
 * Represents 256 bit unsigned integers with standard arithmetic, byte-wise operation and EVM-specific extensions
 * require(n.signum >= 0 && n.compareTo(MODULUS) < 0, s"Invalid EvmWord value: $n") --- already checked in apply(n: BigInteger)
 */
final class EvmWord private (val n: BigInteger) extends Ordered[EvmWord] {
  import EvmWord._

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
   * Converts an EvmWord to an Array[Byte].
   * Output Array[Byte] is padded with zeros from the left side up to EvmWord.Size bytes.
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

  def getByte(that: EvmWord): EvmWord = {
    if (that.n.compareTo(THIRTY_ONE) > 0) {
      Zero
    } else {
      EvmWord.safe(bytes(that.n.intValue).toInt & 0xff)
    }
  }

  // standard arithmetic (note the use of new instead of apply where result is guaranteed to be within bounds)
  def &(that: EvmWord): EvmWord = EvmWord.safe(n and that.n)
  def |(that: EvmWord): EvmWord = EvmWord.safe(n or that.n)
  def ^(that: EvmWord): EvmWord = EvmWord.safe(n xor that.n)
  def unary_- : EvmWord = EvmWord(n.negate)
  def unary_~ : EvmWord = EvmWord(n.not)
  def +(that: EvmWord): EvmWord = EvmWord(n add that.n)
  def -(that: EvmWord): EvmWord = EvmWord(n subtract that.n)
  def *(that: EvmWord): EvmWord = EvmWord(n multiply that.n)
  def /(that: EvmWord): EvmWord = EvmWord.safe(n divide that.n)
  def **(that: EvmWord): EvmWord = EvmWord.safe(n.modPow(that.n, MODULUS))

  def +(that: Int): EvmWord = EvmWord(n add BigInteger.valueOf(that))
  def -(that: Int): EvmWord = EvmWord(n subtract BigInteger.valueOf(that))
  def *(that: Int): EvmWord = EvmWord(n multiply BigInteger.valueOf(that))
  def /(that: Int): EvmWord = EvmWord.safe(n divide BigInteger.valueOf(that))

  def +(that: Long): EvmWord = EvmWord(n add BigInteger.valueOf(that))
  def -(that: Long): EvmWord = EvmWord(n subtract BigInteger.valueOf(that))
  def *(that: Long): EvmWord = EvmWord(n multiply BigInteger.valueOf(that))
  def /(that: Long): EvmWord = EvmWord.safe(n divide BigInteger.valueOf(that))

  def pow(that: Int): EvmWord = EvmWord(n pow that)

  def min(that: EvmWord): EvmWord = if (n.compareTo(that.n) < 0) this else that
  def max(that: EvmWord): EvmWord = if (n.compareTo(that.n) > 0) this else that
  def isZero: Boolean = n.signum == 0
  def nonZero: Boolean = n.signum != 0

  def div(that: EvmWord): EvmWord = if (that.n.signum == 0) Zero else EvmWord.safe(n divide that.n)
  def sdiv(that: EvmWord): EvmWord = if (that.n.signum == 0) Zero else EvmWord(signed divide that.signed)
  def mod(that: EvmWord): EvmWord = if (that.n.signum == 0) Zero else EvmWord(n mod that.n)
  def smod(that: EvmWord): EvmWord = if (that.n.signum == 0) Zero else EvmWord(signed remainder that.signed.abs)
  def addmod(that: EvmWord, modulus: EvmWord): EvmWord = if (modulus.n.signum == 0) Zero else EvmWord.safe((n add that.n) remainder modulus.n)
  def mulmod(that: EvmWord, modulus: EvmWord): EvmWord = if (modulus.n.signum == 0) Zero else EvmWord.safe((n multiply that.n) mod modulus.n)

  def slt(that: EvmWord): Boolean = signed.compareTo(that.signed) < 0
  def sgt(that: EvmWord): Boolean = signed.compareTo(that.signed) > 0

  def signExtend(that: EvmWord): EvmWord = {
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
      EvmWord.safe(newN)
    }
  }

  /**
   * Shift left, both this and input arg are treated as unsigned
   * @param arg
   * @return this << arg
   */
  def shiftLeft(arg: EvmWord): EvmWord = {
    if (arg.n.compareTo(MAX_POW) >= 0) {
      Zero
    } else {
      EvmWord(n.shiftLeft(arg.intValueSafe))
    }
  }

  /**
   * Shift right, both this and input arg are treated as unsigned
   * @param arg
   * @return this >> arg
   */
  def shiftRight(arg: EvmWord): EvmWord = {
    if (arg.n.compareTo(MAX_POW) >= 0) {
      Zero
    } else {
      EvmWord(n.shiftRight(arg.intValueSafe))
    }
  }

  /**
   * Shift right, this is signed, while input arg is treated as unsigned
   * @param arg
   * @return this >> arg
   */
  def shiftRightSigned(arg: EvmWord): EvmWord = {
    if (arg.n.compareTo(MAX_POW) >= 0) {
      if (isNegative) {
        EvmWord(BigInteger.ONE.negate)
      } else {
        Zero
      }
    } else {

      EvmWord(signed.shiftRight(arg.intValueSafe))
    }
  }

  /**
   * only in case of signed operation when the number is explicit defined
   * as negative
   */
  private def isNegative = {
    signed.signum < 0
  }

  def compare(that: EvmWord): Int = n.compareTo(that.n)

  override def equals(any: Any): Boolean = {
    any match {
      case that: EvmWord    => (this eq that) || n == that.n
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
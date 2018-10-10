package khipu.vm

import akka.util.ByteString
import java.math.BigInteger
import khipu.Hash
import khipu.util.BytesUtil
import language.implicitConversions

/**
 * Use new instead of apply to bypass boundBigInt where result is guaranteed to be within bounds
 */
object UInt256 {

  val Size = 32 // size in bytes
  val SizeInBits = 256 // 32 * 8

  val ZERO = BigInteger.ZERO
  val ONE = BigInteger.ONE
  val TWO = BigInteger.valueOf(2)
  val MAX_INT = BigInteger.valueOf(Int.MaxValue)
  val MAX_LONG = BigInteger.valueOf(Long.MaxValue)
  val THIRTY_ONE = BigInteger.valueOf(31)
  val THIRTY_TWO = BigInteger.valueOf(32)
  val TWO_FIVE_SIX = BigInteger.valueOf(256)

  val MODULUS = TWO pow SizeInBits
  val MAX_VALUE = MODULUS subtract ONE
  val MAX_SIGNED_VALUE = (TWO pow (SizeInBits - 1)) subtract ONE

  // UInt256 value should be put behind MODULUS (will be used in UInt256 constructor) etc
  val SIZE = new UInt256(THIRTY_TWO)

  val Zero: UInt256 = new UInt256(ZERO)
  val One: UInt256 = new UInt256(ONE)
  val Two: UInt256 = new UInt256(TWO)
  val TwoFiveSix = new UInt256(TWO_FIVE_SIX)

  def apply(b: Boolean): UInt256 = if (b) One else Zero
  def apply(n: Long): UInt256 = new UInt256(BigInteger.valueOf(n))
  def apply(n: Int): UInt256 = new UInt256(BigInteger.valueOf(n))
  def apply(bytes: ByteString): UInt256 = apply(bytes.toArray)
  def apply(bytes: Hash): UInt256 = apply(bytes.bytes)

  // with bound limited
  def apply(n: BigInteger): UInt256 = new UInt256(boundBigInt(n))
  def apply(bytes: Array[Byte]): UInt256 = {
    require(bytes.length <= Size, s"Input byte array cannot be longer than $Size: ${bytes.length}")
    new UInt256(new BigInteger(1, bytes))
  }

  def safe(n: Int): UInt256 = new UInt256(BigInteger.valueOf(n))
  def safe(n: Long): UInt256 = new UInt256(BigInteger.valueOf(n))

  private def boundBigInt(n: BigInteger): BigInteger = {
    if (n.signum == 0) {
      ZERO
    } else if (n.signum < 0) {
      val unsigned = (n remainder MODULUS) add MODULUS
      if (unsigned.compareTo(MODULUS) == 0) {
        ZERO
      } else {
        unsigned
      }
    } else if (n.compareTo(MODULUS) >= 0) {
      n remainder MODULUS
    } else {
      n
    }
  }

  /**
   * Number of 32-byte UInt256s required to hold n bytes (~= math.ceil(n / 32))
   * We assume n is not neseccary to exceed Long.MaxValue, and use Long here
   */
  def wordsForBytes(n: Long): Long = if (n == 0) 0 else (n - 1) / Size + 1
}

// TODO: consider moving to util as Uint256, which follows Scala numeric conventions, and is used across the system for P_256 numbers (see YP 4.3) [EC-252]
/**
 * Represents 256 bit unsigned integers with standard arithmetic, byte-wise operation and EVM-specific extensions
 * require(n.signum >= 0 && n.compareTo(MODULUS) < 0, s"Invalid UInt256 value: $n") --- already checked in apply(n: BigInteger)
 */
final class UInt256 private (val n: BigInteger) extends Ordered[UInt256] {
  import UInt256._

  // EVM-specific arithmetic
  private lazy val signedN: BigInteger = if (n.compareTo(MAX_SIGNED_VALUE) > 0) n subtract MODULUS else n

  lazy val nonZeroLeadingBytes: Array[Byte] = {
    val src = n.toByteArray
    var i = 0
    while (i < src.length && src(i) == 0) {
      i += 1
    }
    val dest = Array.ofDim[Byte](src.length - i)
    System.arraycopy(src, i, dest, 0, dest.length)
    dest
  }

  /**
   * Converts a BigInteger to a ByteString.
   * Output ByteString is padded with zeros from the left side up to UInt256.Size bytes.
   */
  lazy val bytes: Array[Byte] = {
    val src = n.toByteArray
    if (src.length == Size) {
      src
    } else {
      val dest = Array.fill[Byte](Size)(0)
      val len = math.min(src.length, Size)
      val srcOffset = math.max(0, src.length - len)
      val destOffset = dest.length - len
      System.arraycopy(src, srcOffset, dest, destOffset, len)
      dest
    }
  }

  /**
   * Used for gas calculation for EXP opcode. See YP Appendix H.1 (220)
   * For n > 0: (n.bitLength - 1) / 8 + 1 == 1 + floor(log_256(n))
   *
   * @return Size in bytes excluding the leading 0 bytes
   */
  def byteSize: Int = if (n.signum == 0) 0 else (n.bitLength - 1) / 8 + 1

  def getByte(that: UInt256): UInt256 =
    if (that.n.compareTo(THIRTY_ONE) > 0) Zero else UInt256(bytes(that.n.intValue).toInt & 0xff)

  // standard arithmetic (note the use of new instead of apply where result is guaranteed to be within bounds)
  def &(that: UInt256): UInt256 = new UInt256(n and that.n)
  def |(that: UInt256): UInt256 = new UInt256(n or that.n)
  def ^(that: UInt256): UInt256 = new UInt256(n xor that.n)
  def unary_- : UInt256 = UInt256(n.negate)
  def unary_~ : UInt256 = UInt256(n.not)
  def +(that: UInt256): UInt256 = UInt256(n add that.n)
  def -(that: UInt256): UInt256 = UInt256(n subtract that.n)
  def *(that: UInt256): UInt256 = UInt256(n multiply that.n)
  def /(that: UInt256): UInt256 = new UInt256(n divide that.n)
  def **(that: UInt256): UInt256 = UInt256(n.modPow(that.n, MODULUS))
  def compare(that: UInt256): Int = n.compareTo(that.n)
  def min(that: UInt256): UInt256 = if (n.compareTo(that.n) < 0) this else that
  def max(that: UInt256): UInt256 = if (n.compareTo(that.n) > 0) this else that
  def isZero: Boolean = n.signum == 0
  def nonZero: Boolean = n.signum != 0

  def div(that: UInt256): UInt256 = if (that.n.signum == 0) Zero else new UInt256(n divide that.n)
  def sdiv(that: UInt256): UInt256 = if (that.n.signum == 0) Zero else UInt256(signedN divide that.signedN)
  def mod(that: UInt256): UInt256 = if (that.n.signum == 0) Zero else UInt256(n mod that.n)
  def smod(that: UInt256): UInt256 = if (that.n.signum == 0) Zero else UInt256(signedN remainder that.signedN.abs)
  def addmod(that: UInt256, modulus: UInt256): UInt256 = if (modulus.n.signum == 0) Zero else new UInt256((n add that.n) remainder modulus.n)
  def mulmod(that: UInt256, modulus: UInt256): UInt256 = if (modulus.n.signum == 0) Zero else new UInt256((n multiply that.n) mod modulus.n)

  def slt(that: UInt256): Boolean = signedN.compareTo(that.signedN) < 0
  def sgt(that: UInt256): Boolean = signedN.compareTo(that.signedN) > 0

  def signExtend(that: UInt256): UInt256 = {
    if (that.n.signum < 0 || that.n.compareTo(THIRTY_ONE) > 0) {
      this
    } else {
      val idx = that.n.byteValue
      val negative = n.testBit(idx * 8 + 7)
      val mask = (ONE shiftLeft ((idx + 1) * 8)) subtract ONE
      val newN = if (negative) n or (MAX_VALUE xor mask) else n and mask
      new UInt256(newN)
    }
  }

  def compareTo(that: BigInteger): Int = n.compareTo(that)

  override def equals(that: Any): Boolean = {
    that match {
      case that: UInt256    => n.compareTo(that.n) == 0
      case that: BigInteger => n.compareTo(that) == 0
      case that: Byte       => n.compareTo(BigInteger.valueOf(that)) == 0
      case that: Short      => n.compareTo(BigInteger.valueOf(that)) == 0
      case that: Int        => n.compareTo(BigInteger.valueOf(that)) == 0
      case that: Long       => n.compareTo(BigInteger.valueOf(that)) == 0
      case other            => other == n
    }
  }

  override def hashCode: Int = n.hashCode
  override def toString: String = toSignedDecString

  def toDecString: String =
    n.toString

  def toSignedDecString: String =
    signedN.toString

  def toHexString: String = {
    val hex = f"$n%x"
    // add zero if odd number of digits
    val extraZero = if (hex.length % 2 == 0) "" else "0"
    s"0x$extraZero$hex"
  }

  def toBigInt: BigInteger = n

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
    val occupied = bytesOccupied
    val intVal = intValue
    if (occupied > 4 || intVal < 0) {
      Int.MaxValue
    } else {
      intVal
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
    val occupied = bytesOccupied
    val longVal = longValue
    if (occupied > 8 || longVal < 0) {
      Long.MaxValue
    } else {
      longVal
    }
  }
}

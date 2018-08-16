package khipu.util

import akka.util.ByteString
import java.math.BigInteger
import java.nio.ByteBuffer
import khipu.Hash
import scala.util.Random

object BytesUtil {
  val EMPTY_BYTE_ARRAY = Array[Byte]()
  val ZERO_BYTE_ARRAY = Array[Byte](0)

  def bigIntegerToBytes(b: BigInteger, numBytes: Int): Array[Byte] = {
    val biBytes = b.toByteArray
    val start = if (biBytes.length == numBytes + 1) 1 else 0
    val length = math.min(biBytes.length, numBytes)

    val bytes = Array.ofDim[Byte](numBytes)
    System.arraycopy(biBytes, start, bytes, numBytes - length, length)
    bytes
  }

  def xor(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
    val res = Array.ofDim[Byte](a.length)
    var i = 0
    while (i < a.length) {
      val b1 = a(i)
      val b2 = b(i)
      res(i) = (b1 ^ b2).toByte
      i += 1
    }
    res
  }

  def or(arrays: Array[Byte]*): Array[Byte] = {
    require(arrays.map(_.length).distinct.length <= 1, "All the arrays should have the same length")
    require(arrays.nonEmpty, "There should be one or more arrays")

    val zeroes = Array.fill[Byte](arrays.head.length)(0)
    arrays.foldLeft[Array[Byte]](zeroes) {
      case (acc, array) =>
        var i = 0
        while (i < acc.length) {
          val b1 = acc(i)
          val b2 = array(i)
          acc(i) = (b1 | b2).toByte
          i += 1
        }
        acc
    }
  }

  def and(arrays: Array[Byte]*): Array[Byte] = {
    require(arrays.map(_.length).distinct.length <= 1, "All the arrays should have the same length")
    require(arrays.nonEmpty, "There should be one or more arrays")

    val ones = Array.fill(arrays.head.length)(0xFF.toByte)
    arrays.foldLeft[Array[Byte]](ones) {
      case (acc, array) =>
        var i = 0
        while (i < acc.length) {
          val b1 = acc(i)
          val b2 = array(i)
          acc(i) = (b1 & b2).toByte
          i += 1
        }
        acc
    }
  }

  def randomBytes(len: Int): Array[Byte] = {
    val arr = Array.ofDim[Byte](len)
    new Random().nextBytes(arr)
    arr
  }

  def bigEndianToShort(bs: Array[Byte]): Short = {
    val n = bs(0) << 8
    (n | bs(1) & 0xFF).toShort
  }

  def padLeft(bytes: ByteString, length: Int, byte: Byte): ByteString = {
    ByteString(padLeft(bytes.toArray, length, byte))
  }

  def padLeft(bytes: Hash, length: Int, byte: Byte): Hash = {
    Hash(padLeft(bytes.bytes, length, byte))
  }

  def padLeft(bytes: Array[Byte], length: Int, byte: Byte): Array[Byte] = {
    val len = math.max(0, length - bytes.length)
    val pads = Array.fill[Byte](len)(byte)
    concat(pads, bytes)
  }

  def compactPickledBytes(buffer: ByteBuffer): ByteString = {
    val bytes = Array.ofDim[Byte](buffer.limit)
    buffer.rewind()
    buffer.get(bytes)
    ByteString(bytes)
  }

  def bytesToIp(bytesIp: ByteString): String = {
    val sb = new StringBuilder()
    sb.append(bytesIp(0) & 0xFF)
    sb.append(".")
    sb.append(bytesIp(1) & 0xFF)
    sb.append(".")
    sb.append(bytesIp(2) & 0xFF)
    sb.append(".")
    sb.append(bytesIp(3) & 0xFF)
    sb.toString()
  }

  /**
   * Parses fixed number of bytes starting from {@code offset} in {@code input} array.
   * If {@code input} has not enough bytes return array will be right padded with zero bytes.
   * I.e. if {@code offset} is higher than {@code input.length} then zero byte array of length {@code len} will be returned
   */
  def parseBytes(input: ByteString, offset: Int, len: Int): Array[Byte] = {
    if (offset >= input.length || len == 0) {
      EMPTY_BYTE_ARRAY
    } else {
      val bytes = Array.ofDim[Byte](len)
      System.arraycopy(input.toArray, offset, bytes, 0, math.min(input.length - offset, len))
      bytes
    }

  }

  /**
   * Parses 32-bytes word from given input.
   * Uses {@link #parseBytes(byte[], int, int)} method,
   * thus, result will be right-padded with zero bytes if there is not enough bytes in {@code input}
   *
   * @param idx an index of the word starting from {@code 0}
   */
  def parseWord(input: ByteString, idx: Int): Array[Byte] = {
    parseBytes(input, 32 * idx, 32)
  }

  /**
   * Parses 32-bytes word from given input.
   * Uses {@link #parseBytes(byte[], int, int)} method,
   * thus, result will be right-padded with zero bytes if there is not enough bytes in {@code input}
   *
   * @param idx an index of the word starting from {@code 0}
   * @param offset an offset in {@code input} array to start parsing from
   */
  def parseWord(input: ByteString, offset: Int, idx: Int): Array[Byte] = {
    parseBytes(input, offset + 32 * idx, 32)
  }

  /**
   * Returns a number of zero bits preceding the highest-order ("leftmost") one-bit
   * interpreting input array as a big-endian integer value
   */
  def numberOfLeadingZeros(bytes: Array[Byte]): Int = {
    val i = firstNonZeroByte(bytes)
    if (i == -1) {
      bytes.length * 8
    } else {
      val byteLeadingZeros = Integer.numberOfLeadingZeros(bytes(i) & 0xff) - 24
      i * 8 + byteLeadingZeros
    }
  }

  def firstNonZeroByte(data: Array[Byte]): Int = {
    var i = 0
    while (i < data.length) {
      if (data(i) != 0) {
        return i
      }
      i += 1
    }
    -1
  }

  def stripLeadingZeroes(data: Array[Byte]): Array[Byte] = {
    if (data == null)
      return null

    firstNonZeroByte(data) match {
      case -1 => ZERO_BYTE_ARRAY
      case 0  => data
      case x =>
        val result = Array.ofDim[Byte](data.length - x)
        System.arraycopy(data, x, result, 0, data.length - x)

        result
    }
  }

  def bytesToBigInteger(bb: Array[Byte]): BigInteger = {
    if (bb.length == 0) BigInteger.ZERO else new BigInteger(1, bb)
  }

  def tail(bytes: Array[Byte]): Array[Byte] = {
    val tail = Array.ofDim[Byte](bytes.length - 1)
    System.arraycopy(bytes, 1, tail, 0, tail.length)
    tail
  }

  def drop(byte: Array[Byte], pos: Int): Array[Byte] = {
    val dropped = Array.ofDim[Byte](byte.length - pos)
    System.arraycopy(byte, pos, dropped, 0, dropped.length)
    dropped
  }

  def split(bytes: Array[Byte], pos: Int): (Array[Byte], Array[Byte]) = {
    val first = Array.ofDim[Byte](pos)
    val second = Array.ofDim[Byte](bytes.length - pos)
    System.arraycopy(bytes, 0, first, 0, first.length)
    System.arraycopy(bytes, first.length, second, 0, second.length)
    (first, second)
  }

  def concat(bytes1: Array[Byte], bytes2: Array[Byte]): Array[Byte] = {
    val concat = Array.ofDim[Byte](bytes1.length + bytes2.length)
    System.arraycopy(bytes1, 0, concat, 0, bytes1.length)
    System.arraycopy(bytes2, 0, concat, bytes1.length, bytes2.length)
    concat
  }
}

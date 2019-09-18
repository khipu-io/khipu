package khipu.util

import akka.util.ByteString
import java.math.BigInteger
import java.nio.ByteBuffer
import khipu.Hash
import scala.annotation.switch
import scala.util.Random

object BytesUtil {
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

  def or(_arrays: Array[Byte]*): Array[Byte] = {
    val arrays = _arrays.toArray
    require(arrays.nonEmpty, "There should be one or more arrays")
    require(arrays.map(_.length).distinct.length <= 1, "All the arrays should have the same length")

    val zeroes = Array.ofDim[Byte](arrays.head.length) // auto filled with 0
    var i = 0
    while (i < arrays.length) {
      var k = 0
      while (k < zeroes.length) {
        val b1 = zeroes(k)
        val b2 = arrays(i)(k)
        zeroes(k) = (b1 | b2).toByte
        k += 1
      }
      i += 1
    }
    zeroes
  }

  def and(_arrays: Array[Byte]*): Array[Byte] = {
    val arrays = _arrays.toArray
    require(arrays.nonEmpty, "There should be one or more arrays")
    require(arrays.map(_.length).distinct.length <= 1, "All the arrays should have the same length")

    val ones = Array.fill(arrays.head.length)(0xFF.toByte)
    var i = 0
    while (i < arrays.length) {
      var k = 0
      while (k < ones.length) {
        val b1 = ones(k)
        val b2 = arrays(i)(k)
        ones(k) = (b1 & b2).toByte
        k += 1
      }
      i += 1
    }
    ones
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
    val pads = if (byte == 0) {
      Array.ofDim[Byte](len) // auto filled with 0
    } else {
      Array.fill[Byte](len)(byte)
    }
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
      Array.emptyByteArray
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
    if (data == null) {
      null
    } else {
      (firstNonZeroByte(data): @switch) match {
        case -1 => ZERO_BYTE_ARRAY
        case 0  => data
        case x =>
          val res = Array.ofDim[Byte](data.length - x)
          System.arraycopy(data, x, res, 0, data.length - x)
          res
      }
    }
  }

  def bytesToBigInteger(bytes: Array[Byte]): BigInteger = {
    if (bytes.length == 0) BigInteger.ZERO else new BigInteger(1, bytes)
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

  def intsToBytes(arr: Array[Int], bigEndian: Boolean): Array[Byte] = {
    val ret = Array.ofDim[Byte](arr.length * 4)
    intsToBytes(arr, ret, bigEndian)
    ret
  }

  def bytesToInts(arr: Array[Byte], bigEndian: Boolean): Array[Int] = {
    val ret = Array.ofDim[Int](arr.length / 4)
    bytesToInts(arr, ret, bigEndian)
    ret
  }

  def bytesToInts(b: Array[Byte], arr: Array[Int], bigEndian: Boolean) {
    if (!bigEndian) {
      var off = 0
      var i = 0
      while (i < arr.length) {
        var ii = b(off) & 0x000000FF
        off += 1
        ii |= (b(off) << 8) & 0x0000FF00
        off += 1
        ii |= (b(off) << 16) & 0x00FF0000
        off += 1
        ii |= (b(off) << 24)
        off += 1

        arr(i) = ii
        i += 1
      }
    } else {
      var off = 0;
      var i = 0
      while (i < arr.length) {
        var ii = b(off) << 24
        off += 1
        ii |= (b(off) << 16) & 0x00FF0000
        off += 1
        ii |= (b(off) << 8) & 0x0000FF00
        off += 1
        ii |= b(off) & 0x000000FF
        off += 1

        arr(i) = ii
        i += 1
      }
    }
  }

  def intsToBytes(arr: Array[Int], b: Array[Byte], bigEndian: Boolean) {
    if (!bigEndian) {
      var off = 0;
      var i = 0
      while (i < arr.length) {
        val ii = arr(i)

        b(off) = (ii & 0xFF).toByte
        off += 1
        b(off) = ((ii >> 8) & 0xFF).toByte
        off += 1
        b(off) = ((ii >> 16) & 0xFF).toByte
        off += 1
        b(off) = ((ii >> 24) & 0xFF).toByte
        off += 1

        i += 1
      }
    } else {
      var off = 0
      var i = 0
      while (i < arr.length) {
        val ii = arr(i)

        b(off) = ((ii >> 24) & 0xFF).toByte
        off += 1
        b(off) = ((ii >> 16) & 0xFF).toByte
        off += 1
        b(off) = ((ii >> 8) & 0xFF).toByte
        off += 1
        b(off) = (ii & 0xFF).toByte
        off += 1

        i += 1
      }
    }
  }

  def longToBytes(v: Long): Array[Byte] = {
    ByteBuffer.allocate(java.lang.Long.BYTES).putLong(v).array
  }

  def merge(arrays: Array[Byte]*): Array[Byte] = {
    val count = arrays.map(_.length).sum

    // Create new array and copy all array contents
    val mergedArray = Array.ofDim[Byte](count)
    var start = 0
    for (array <- arrays) {
      System.arraycopy(array, 0, mergedArray, start, array.length)
      start += array.length
    }
    mergedArray
  }
}

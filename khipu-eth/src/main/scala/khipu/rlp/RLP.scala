package khipu.rlp

import scala.annotation.switch
import scala.annotation.tailrec

/**
 * Recursive Length Prefix (RLP) encoding.
 * <p>
 * The purpose of RLP is to encode arbitrarily nested arrays of binary data, and
 * RLP is the main encoding method used to serialize objects in Ethereum. The
 * only purpose of RLP is to encode structure; encoding specific atomic data
 * types (eg. strings, integers, floats) is left up to higher-order protocols; in
 * Ethereum the standard is that integers are represented in big endian binary
 * form. If one wishes to use RLP to encode a dictionary, the two suggested
 * canonical forms are to either use &#91;&#91;k1,v1],[k2,v2]...] with keys in
 * lexicographic order or to use the higher-level Patricia Tree encoding as
 * Ethereum does.
 * <p>
 * The RLP encoding function takes in an item. An item is defined as follows:
 * <p>
 * - A string (ie. byte array) is an item - A list of items is an item
 * <p>
 * For example, an empty string is an item, as is the string containing the word
 * "cat", a list containing any number of strings, as well as more complex data
 * structures like ["cat",["puppy","cow"],"horse",[[]],"pig",[""],"sheep"]. Note
 * that in the context of the rest of this article, "string" will be used as a
 * synonym for "a certain number of bytes of binary data"; no special encodings
 * are used and no knowledge about the content of the strings is implied.
 * <p>
 * See: https://github.com/ethereum/wiki/wiki/%5BEnglish%5D-RLP
 *
 */
private[rlp] object RLP {

  private[rlp] final case class ItemBounds(start: Int, end: Int, isList: Boolean, isEmpty: Boolean = false)
  /**
   * Reason for threshold according to Vitalik Buterin:
   * - 56 bytes maximizes the benefit of both options
   * - if we went with 60 then we would have only had 4 slots for long strings
   * so RLP would not have been able to store objects above 4gb
   * - if we went with 48 then RLP would be fine for 2^128 space, but that's way too much
   * - so 56 and 2^64 space seems like the right place to put the cutoff
   * - also, that's where Bitcoin's varint does the cutof
   */
  private val SizeThreshold: Int = 56

  /**
   * Allow for content up to size of 2&#94;64 bytes *
   */
  private val MaxItemLength: Double = math.pow(256, 8)

  /** RLP encoding rules are defined as follows: */

  /*
   * For a single byte whose value is in the [0x00, 0x7f] range, that byte is
   * its own RLP encoding.
   */

  /**
   * [0x80]
   * If a string is 0-55 bytes long, the RLP encoding consists of a single
   * byte with value 0x80 plus the length of the string followed by the
   * string. The range of the first byte is thus [0x80, 0xb7].
   */
  private val OffsetShortItem: Int = 0x80

  /**
   * [0xb7]
   * If a string is more than 55 bytes long, the RLP encoding consists of a
   * single byte with value 0xb7 plus the length of the length of the string
   * in binary form, followed by the length of the string, followed by the
   * string. For example, a length-1024 string would be encoded as
   * \xb9\x04\x00 followed by the string. The range of the first byte is thus
   * [0xb8, 0xbf].
   */
  private val OffsetLongItem: Int = 0xb7

  /**
   * [0xc0]
   * If the total payload of a list (i.e. the combined length of all its
   * items) is 0-55 bytes long, the RLP encoding consists of a single byte
   * with value 0xc0 plus the length of the list followed by the concatenation
   * of the RLP encodings of the items. The range of the first byte is thus
   * [0xc0, 0xf7].
   */
  private val OffsetShortList: Int = 0xc0

  /**
   * [0xf7]
   * If the total payload of a list is more than 55 bytes long, the RLP
   * encoding consists of a single byte with value 0xf7 plus the length of the
   * length of the list in binary form, followed by the length of the list,
   * followed by the concatenation of the RLP encodings of the items. The
   * range of the first byte is thus [0xf8, 0xff].
   */
  private val OffsetLongList = 0xf7

  /**
   * This functions decodes an RLP encoded Array[Byte] without converting it to any specific type. This method should
   * be faster (as no conversions are done)
   *
   * @param data RLP Encoded instance to be decoded
   * @return A RLPEncodeable
   * @throws RLPException if there is any error
   */
  private[rlp] def rawDecode(data: Array[Byte]): RLPEncodeable = decodeWithPos(data, 0)._1

  /**
   * This function encodes an RLPEncodeable instance
   *
   * @param input RLP Instance to be encoded
   * @return A byte array with item encoded
   */
  private[rlp] def encode(input: RLPEncodeable): Array[Byte] = {
    input match {
      case list: RLPList =>
        val output = list.items.foldLeft(Array[Byte]()) { (acc, item) =>
          val encoded = encode(item)
          val acc1 = Array.ofDim[Byte](acc.length + encoded.length)
          System.arraycopy(acc, 0, acc1, 0, acc.length)
          System.arraycopy(encoded, 0, acc1, acc.length, encoded.length)
          acc1
        }
        val binaryLen = encodeLength(output.length, OffsetShortList)

        val payload = Array.ofDim[Byte](binaryLen.length + output.length)
        System.arraycopy(binaryLen, 0, payload, 0, binaryLen.length)
        System.arraycopy(output, 0, payload, binaryLen.length, output.length)
        payload

      case value: RLPValue =>
        value.bytes match {
          case bytes @ Array(b) if (b & 0xff) < 0x80 => bytes
          case bytes =>
            val binaryLen = encodeLength(bytes.length, OffsetShortItem)
            val payload = Array.ofDim[Byte](binaryLen.length + bytes.length)
            System.arraycopy(binaryLen, 0, payload, 0, binaryLen.length)
            System.arraycopy(bytes, 0, payload, binaryLen.length, bytes.length)
            payload
        }
    }
  }

  /**
   * Integer limitation goes up to 2&#94;31-1 so length can never be bigger than MAX_ITEM_LENGTH
   */
  private def encodeLength(length: Int, offset: Int): Array[Byte] = {
    if (length < SizeThreshold) {
      Array((length + offset).toByte)
    } else if (length < MaxItemLength && length > 0xFF) {
      val binaryLen = intToBytesNoLeadZeroes(length)
      val payload = Array.ofDim[Byte](1 + binaryLen.length)
      payload(0) = (binaryLen.length + offset + SizeThreshold - 1).toByte
      System.arraycopy(binaryLen, 0, payload, 1, binaryLen.length)
      payload
    } else if (length < MaxItemLength && length <= 0xFF) {
      Array((1 + offset + SizeThreshold - 1).toByte, length.toByte)
    } else throw RLPException("Input too long")
  }

  /**
   * This function calculates, based on RLP definition, the bounds of a single value.
   *
   * @param data An Array[Byte] containing the RLP item to be searched
   * @param pos  Initial position to start searching
   * @return Item Bounds description
   * @see [[khipu.rlp.ItemBounds]]
   */
  private[rlp] def getItemBounds(data: Array[Byte], pos: Int): ItemBounds = {
    if (data.length == 0) {
      throw RLPException("Empty Data")
    } else {
      val prefix = data(pos) & 0xFF
      if (prefix == OffsetShortItem) {
        ItemBounds(start = pos, end = pos, isList = false, isEmpty = true)
      } else if (prefix < OffsetShortItem)
        ItemBounds(start = pos, end = pos, isList = false)
      else if (prefix <= OffsetLongItem) {
        val length = prefix - OffsetShortItem
        ItemBounds(start = pos + 1, end = pos + length, isList = false)
      } else if (prefix < OffsetShortList) {
        val lengthOfLength = prefix - OffsetLongItem
        val lengthBytes = slice(data, pos + 1, pos + 1 + lengthOfLength)
        val length = bigEndianMinLengthToInt(lengthBytes)
        val beginPos = pos + 1 + lengthOfLength
        ItemBounds(start = beginPos, end = beginPos + length - 1, isList = false)
      } else if (prefix <= OffsetLongList) {
        val length = prefix - OffsetShortList
        ItemBounds(start = pos + 1, end = pos + length, isList = true)
      } else {
        val lengthOfLength = prefix - OffsetLongList
        val lengthBytes = slice(data, pos + 1, pos + 1 + lengthOfLength)
        val length = bigEndianMinLengthToInt(lengthBytes)
        val beginPos = pos + 1 + lengthOfLength
        ItemBounds(start = beginPos, end = beginPos + length - 1, isList = true)
      }
    }
  }

  private def decodeWithPos(data: Array[Byte], pos: Int): (RLPEncodeable, Int) =
    if (data.length == 0) {
      throw RLPException("Empty Data")
    } else {
      getItemBounds(data, pos) match {
        case ItemBounds(start, end, false, isEmpty) =>
          RLPValue(if (isEmpty) Array[Byte]() else slice(data, start, end + 1)) -> (end + 1)
        case ItemBounds(start, end, true, _) =>
          RLPList(decodeListRecursive(data, start, end - start + 1, Vector()): _*) -> (end + 1)
      }
    }

  @tailrec
  private def decodeListRecursive(data: Array[Byte], pos: Int, length: Int, acum: Vector[RLPEncodeable]): Vector[RLPEncodeable] = {
    if (length == 0) {
      acum
    } else {
      val (decoded, decodedEnd) = decodeWithPos(data, pos)
      decodeListRecursive(data, decodedEnd, length - (decodedEnd - pos), acum :+ decoded)
    }
  }

  /**
   * This function transform a byte into byte array
   *
   * @param singleByte to encode
   * @return encoded bytes
   */
  private[rlp] def byteToByteArray(singleByte: Byte): Array[Byte] = {
    if ((singleByte & 0xFF) == 0) {
      Array[Byte]()
    } else {
      Array(singleByte)
    }
  }

  /**
   * This function converts a short value to a big endian byte array of minimal length
   *
   * @param singleShort value to encode
   * @return encoded bytes
   */
  private[rlp] def shortToBigEndianMinLength(singleShort: Short): Array[Byte] = {
    if ((singleShort & 0xFF) == singleShort) {
      byteToByteArray(singleShort.toByte)
    } else {
      Array((singleShort >> 8 & 0xFF).toByte, (singleShort >> 0 & 0xFF).toByte)
    }
  }

  /**
   * This function converts an int value to a big endian byte array of minimal length
   *
   * @param singleInt value to encode
   * @return encoded bytes
   */
  private[rlp] def intToBigEndianMinLength(singleInt: Int): Array[Byte] = {
    if (singleInt == (singleInt & 0xFF)) {
      byteToByteArray(singleInt.toByte)
    } else if (singleInt == (singleInt & 0xFFFF)) {
      shortToBigEndianMinLength(singleInt.toShort)
    } else if (singleInt == (singleInt & 0xFFFFFF)) {
      Array((singleInt >>> 16).toByte, (singleInt >>> 8).toByte, singleInt.toByte)
    } else {
      Array((singleInt >>> 24).toByte, (singleInt >>> 16).toByte, (singleInt >>> 8).toByte, singleInt.toByte)
    }
  }

  /**
   * This function converts from a big endian byte array of minimal length to an int value
   *
   * @param bytes encoded bytes
   * @return Int value
   * @throws RLPException If the value cannot be converted to a valid int
   */
  private[rlp] def bigEndianMinLengthToInt(bytes: Array[Byte]): Int = {
    (bytes.length: @switch) match {
      case 0 => 0: Short
      case 1 => bytes(0) & 0xFF
      case 2 => ((bytes(0) & 0xFF) << 8) + (bytes(1) & 0xFF)
      case 3 => ((bytes(0) & 0xFF) << 16) + ((bytes(1) & 0xFF) << 8) + (bytes(2) & 0xFF)
      case 4 => ((bytes(0) & 0xFF) << 24) + ((bytes(1) & 0xFF) << 16) + ((bytes(2) & 0xFF) << 8) + (bytes(3) & 0xFF)
      case _ => throw RLPException("Bytes don't represent an int")
    }
  }

  /**
   * Converts a int value into a byte array.
   *
   * @param value - int value to convert
   * @return value with leading byte that are zeroes striped
   */
  private def intToBytesNoLeadZeroes(value: Int): Array[Byte] = {
    val bytes = java.nio.ByteBuffer.allocate(4).putInt(value).array
    var pos = -1
    var i = 0
    while (i < bytes.length && pos == -1) {
      if (bytes(i) != 0) {
        pos = i
      } else {
        i += 1
      }
    }

    if (pos == -1) {
      bytes
    } else if (pos == bytes.length - 1) {
      Array()
    } else {
      val len = bytes.length - pos
      val res = Array.ofDim[Byte](len)
      System.arraycopy(bytes, pos, res, 0, len)
      res
    }
  }

  private def slice(bytes: Array[Byte], from: Int, until: Int): Array[Byte] = {
    val lo = math.max(from, 0)
    val hi = math.min(math.max(until, 0), bytes.length)
    val size = math.max(hi - lo, 0)
    val result = Array.ofDim[Byte](size)
    if (size > 0) {
      System.arraycopy(bytes, lo, result, 0, size)
    }
    result
  }
}


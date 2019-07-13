package khipu.trie

object HexPrefix {
  /**
   * Pack nibbles to binary
   *
   * @param nibbles sequence
   * @param isLeaf boolean used to encode whether or not the data being encoded corresponds to a LeafNode or an ExtensionNode
   * @return hex-encoded byte array
   */
  def encode(nibbles: Array[Byte], isLeaf: Boolean): Array[Byte] = {
    val hasOddLength = nibbles.length % 2 == 1
    val firstByteFlag = (2 * (if (isLeaf) 1 else 0) + (if (hasOddLength) 1 else 0)).toByte
    val lengthFlag = if (hasOddLength) 1 else 2

    val nibblesWithFlag = Array.ofDim[Byte](nibbles.length + lengthFlag)
    System.arraycopy(nibbles, 0, nibblesWithFlag, lengthFlag, nibbles.length)
    nibblesWithFlag(0) = firstByteFlag
    if (!hasOddLength) nibblesWithFlag(1) = 0
    nibblesToBytes(nibblesWithFlag)
  }

  /**
   * Unpack a binary string to its nibbles equivalent
   *
   * @param src of binary data
   * @return array of nibbles in byte-format and
   *         boolean used to encode whether or not the data being decoded corresponds to a LeafNode or an ExtensionNode
   */
  def decode(src: Array[Byte]): (Array[Byte], Boolean) = {
    val srcNibbles = bytesToNibbles(src)
    val t = (srcNibbles(0) & 2) != 0
    val hasOddLength = (srcNibbles(0) & 1) != 0
    val flagLength = if (hasOddLength) 1 else 2

    val res = Array.ofDim[Byte](srcNibbles.length - flagLength)
    System.arraycopy(srcNibbles, flagLength, res, 0, srcNibbles.length - flagLength)
    (res, t)
  }

  /**
   * Transforms an array of 8bit values to the corresponding array of 2 * 4bit values (hexadecimal format)
   *
   * @param bytes byte[]
   * @return array with each individual nibble
   */
  def bytesToNibbles(bytes: Array[Byte]): Array[Byte] = {
    val nibbles = Array.ofDim[Byte](bytes.length * 2)
    var i = 0
    var j = 0
    while (i < bytes.length) {
      val byte = bytes(i)

      nibbles(j) = ((byte >> 4) & 0xF).toByte
      j += 1
      nibbles(j) = (byte & 0xF).toByte
      j += 1

      i += 1
    }
    nibbles
  }

  /**
   * Transforms an array of 4bit values (hexadecimal format) to the corresponding array of 8bit values
   *
   * @param nibbles byte[]
   * @return array with bytes combining pairs of nibbles
   */
  def nibblesToBytes(nibbles: Array[Byte]): Array[Byte] = {
    //require(nibbles.length % 2 == 0)
    val bytes = Array.ofDim[Byte](nibbles.length / 2)
    var i = 0
    var j = 0
    while (i < bytes.length) {
      val n1 = nibbles(j)
      j += 1
      val n2 = nibbles(j)
      j += 1

      bytes(i) = (16 * n1 + n2).toByte
      i += 1
    }
    bytes
  }
}

package kesque

object KesqueIndex {
  val KEY_SIZE = 8
  val VAL_SIZE = 8 // long, offset

  def toShortKey(bytes: Array[Byte]) = {
    val slice = Array.ofDim[Byte](KEY_SIZE)
    val len = math.min(bytes.length, KEY_SIZE)
    System.arraycopy(bytes, bytes.length - len, slice, 0, len)
    slice
  }

  // --- methods for transforming offsets for 2 bytes (16 bits) refer count

  def toMixedOffset(referCount: Int, offset: Long) = {
    val referCountBits = referCount.toLong << 48
    referCountBits | offset
  }

  def toReferCountAndOffset(mixedOffset: Long): (Int, Long) = {
    val referCount = mixedOffset >>> 48
    val offset = mixedOffset & 0x0000FFFFFFFFFFFFL
    (referCount.toInt, offset)
  }
}
trait KesqueIndex {
  def get(key: Array[Byte]): List[Long]
  def put(key: Array[Byte], offset: Long): Unit
  def put(kvs: Iterable[(Array[Byte], Long)]): Unit
  def remove(key: Array[Byte], offset: Long): Unit

  def count: Long

  def stop(): Unit
}

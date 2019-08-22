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
}
trait KesqueIndex {
  import KesqueIndex._

  def get(key: Array[Byte]): List[Long]
  def put(key: Array[Byte], offset: Long): Unit
  def put(kvs: Iterable[(Array[Byte], Long)]): Unit
  def remove(key: Array[Byte], offset: Long): Unit
}

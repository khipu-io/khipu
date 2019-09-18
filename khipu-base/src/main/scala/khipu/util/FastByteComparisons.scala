package khipu.util

object FastByteComparisons {

  def equal(b1: Array[Byte], b2: Array[Byte]): Boolean = {
    b1.length == b2.length && compareTo(b1, 0, b1.length, b2, 0, b2.length) == 0
  }

  /**
   * Lexicographically compare two byte arrays.
   *
   * @param b1 buffer1
   * @param s1 offset1
   * @param l1 length1
   * @param b2 buffer2
   * @param s2 offset2
   * @param l2 length2
   * @return int
   */
  def compareTo(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int): Int = {
    LexicographicalComparerHolder.BEST_COMPARER.compareTo(b1, s1, l1, b2, s2, l2)
  }

  private trait Comparer[T] {
    def compareTo(buffer1: T, offset1: Int, length1: Int, buffer2: T, offset2: Int, length2: Int): Int
  }

  /**
   *
   * <p>Uses reflection to gracefully fall back to the Java implementation if
   * {@code Unsafe} isn't available.
   */
  private object LexicographicalComparerHolder {

    val BEST_COMPARER = PureJavaComparer

    object PureJavaComparer extends Comparer[Array[Byte]] {
      override def compareTo(
        buffer1: Array[Byte], offset1: Int, length1: Int,
        buffer2: Array[Byte], offset2: Int, length2: Int
      ): Int = {
        if ((buffer1 eq buffer2) && offset1 == offset2 && length1 == length2) {
          0
        } else {
          val end1 = offset1 + length1
          val end2 = offset2 + length2
          var i = offset1
          var j = offset2
          while (i < end1 && j < end2) {
            val a = (buffer1(i) & 0xff)
            val b = (buffer2(j) & 0xff)
            if (a != b) {
              return a - b
            }
            i += 1
            j += 1
          }
          length1 - length2
        }
      }
    }
  }

}

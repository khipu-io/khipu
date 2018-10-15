package khipu.vm

import khipu.UInt256
import scala.collection.mutable.ArrayBuffer

object Stack {
  /**
   * Stack max size as defined in the YP (9.1)
   */
  val DefaultMaxSize = 1024

  def empty(maxSize: Int = DefaultMaxSize): Stack = new Stack(maxSize)
}

//TODO: consider a List with head being top of the stack (DUP,SWAP go at most the depth of 16) [EC-251]
/**
 * Stack for the EVM. Instruction pop their arguments from it and push their results to it.
 * The Stack doesn't handle overflow and underflow errors. Any operations that trascend given stack bounds will
 * return the stack unchanged. Pop will always return zeroes in such case.
 */
final class Stack private (val maxSize: Int) {
  private val underlying: ArrayBuffer[UInt256] = ArrayBuffer()

  def push(word: UInt256) {
    if (underlying.length + 1 <= maxSize) {
      underlying += word
    }
  }

  /**
   * Push a sequence of elements to the stack. That last element of the sequence will be the top-most element
   * in the resulting stack
   */
  def push(words: Vector[UInt256]) {
    if (underlying.length + words.size <= maxSize) {
      underlying ++= words
    }
  }

  def pop(): List[UInt256] = {
    val len = underlying.length
    if (len > 0) {
      List(underlying.remove(len - 1))
    } else {
      List(UInt256.Zero)
    }
  }

  /**
   * Pop n elements from the stack. The first element in the resulting sequence will be the top-most element
   * in the current stack
   */
  def pop(n: Int): List[UInt256] = {
    val len = underlying.length
    if (n > 0 && n <= len) {
      var res = List[UInt256]()
      var i = n
      while (i >= 1) {
        res ::= underlying(len - i)
        i -= 1
      }
      underlying.remove(len - n, n)
      res
    } else {
      List.fill(n)(UInt256.Zero)
    }
  }

  /**
   * Duplicate i-th element of the stack, pushing it to the top. i=0 is the top-most element.
   */
  def dup(i: Int) {
    val len = underlying.length
    if (i >= 0 && i < len && len < maxSize) {
      underlying += underlying(len - i - 1)
    }
  }

  /**
   * Swap i-th and the top-most elements of the stack. i=0 is the top-most element (and that would be a no-op)
   */
  def swap(i: Int) {
    val len = underlying.length
    if (i > 0 && i < len) {
      val j = len - i - 1
      val top = underlying(len - 1)
      val nth = underlying(j)
      underlying(len - 1) = nth
      underlying(j) = top
    }
  }

  def size: Int = underlying.size

  /**
   * @return the elements of the stack as a sequence, with the top-most element of the stack
   *         as the first element in the sequence
   */
  def toSeq: Seq[UInt256] = underlying.reverse

  override def equals(that: Any): Boolean = that match {
    case that: Stack => this.underlying sameElements that.underlying
    case _           => false
  }

  override def hashCode(): Int = underlying.hashCode

  override def toString: String = underlying.reverse.mkString("Stack(", ",", ")")
}

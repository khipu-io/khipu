package khipu.vm

import khipu.DataWord

object Stack {

  // -- simple test
  def main(xs: Array[String]) {
    val s = empty()

    (1 to 11) foreach { n => s.push(DataWord.safe(n)) }
    println(s)

    (1 to 12) foreach { n =>
      println(s.pop())
    }
    println(s"size after pop() ${s.size}")

    (1 to 11) foreach { n => s.push(DataWord.safe(n)) }
    (1 to 11) foreach { n =>
      println(s.pop(2))
    }
    println(s"size after pop(2) ${s.size}")

    (1 to 11) foreach { n => s.push(Vector(DataWord.safe(n), DataWord.safe(-n))) }
    println(s)

    s.swap(10)
    println(s)

    s.dup(10)
    println(s)
  }

  /**
   * Stack max size as defined in the YP (9.1)
   */
  val DefaultMaxSize = 1024
  val ListOfZero = List(DataWord.Zero)

  def empty(maxSize: Int = DefaultMaxSize): Stack = new Stack(maxSize)
}

//TODO: consider a List with head being top of the stack (DUP,SWAP go at most the depth of 16) [EC-251]
/**
 * Stack for the EVM. Instruction pop their arguments from it and push their results to it.
 * The Stack doesn't handle overflow and underflow errors. Any operations that trascend given stack bounds will
 * return the stack unchanged. Pop will always return zeroes in such case.
 */
final class Stack private (val maxSize: Int) {
  // store elements in array, the top element is at the tail i.e. (length - 1)
  private var underlying: Array[DataWord] = Array.ofDim[DataWord](16) // initial size 16
  private var length = 0

  private def expend(toSize: Int) {
    val len = (toSize / 16 + 1) * 16
    val xs = Array.ofDim[DataWord](len)
    System.arraycopy(underlying, 0, xs, 0, length)
    underlying = xs
  }

  def push(word: DataWord) {
    if (length + 1 <= maxSize) {
      if (underlying.length < length + 1) {
        expend(length + 1)
      }
      underlying(length) = word
      length += 1
    }
  }

  /**
   * Push a sequence of elements to the stack. That last element of the sequence will be the top-most element
   * in the resulting stack
   */
  def push(words: Vector[DataWord]) {
    val n = words.size
    if (length + n <= maxSize) {
      if (underlying.length < length + n) {
        expend(length + n)
      }
      var i = 0
      val itr = words.iterator
      while (itr.hasNext) {
        underlying(length + i) = itr.next
        i += 1
      }
      length += n
    }
  }

  def pop(): List[DataWord] = {
    if (length > 0) {
      length -= 1
      List(underlying(length))
    } else {
      List(DataWord.Zero)
    }
  }

  /**
   * Pop n elements from the stack. The first element in the resulting sequence will be the top-most element
   * in the current stack
   */
  def pop(n: Int): List[DataWord] = {
    if (n > 0 && n <= length) {
      var res = List[DataWord]()
      var i = n
      while (i >= 1) {
        res ::= underlying(length - i)
        i -= 1
      }
      length -= n
      res
    } else {
      List.fill(n)(DataWord.Zero)
    }
  }

  /**
   * Duplicate i-th element of the stack, pushing it to the top. i=0 is the top-most element.
   */
  def dup(i: Int) {
    if (i >= 0 && i < length && length < maxSize) {
      push(underlying(length - i - 1))
    }
  }

  /**
   * Swap i-th and the top-most elements of the stack. i=0 is the top-most element (and that would be a no-op)
   */
  def swap(i: Int) {
    if (i > 0 && i < length) {
      val k = length - i - 1
      val top = underlying(length - 1)
      val nth = underlying(k)
      underlying(length - 1) = nth
      underlying(k) = top
    }
  }

  def size: Int = length

  /**
   * @return the elements of the stack as a sequence, with the top-most element of the stack
   *         as the first element in the sequence
   */
  def toSeq: Seq[DataWord] = underlying.take(length).reverse

  override def equals(any: Any): Boolean = {
    any match {
      case that: Stack => (this eq that) || (this.underlying.take(length) sameElements that.underlying.take(length))
      case _           => false
    }
  }

  override def hashCode(): Int = underlying.hashCode

  override def toString: String = toSeq.mkString("Stack(", ",", ")")
}

package khipu.vm

import akka.util.ByteString
import java.util.Arrays
import khipu.UInt256

/**
 * Volatile memory with 256 bit address space.
 * Every mutating operation on a Memory returns a new updated copy of it.
 *
 * Related reading:
 * https://solidity.readthedocs.io/en/latest/frequently-asked-questions.html#what-is-the-memory-keyword-what-does-it-do
 * https://github.com/ethereum/go-ethereum/blob/master/core/vm/memory.go
 */
object Memory {
  def empty(): Memory = new Memory()
}
final class Memory private () {
  private var underlying: Array[Byte] = Array[Byte]()

  def store(offset: Int, b: Byte): Unit = store(offset, Array(b))
  def store(offset: Int, uint: UInt256): Unit = store(offset, uint.bytes)
  def store(offset: Int, bytes: ByteString): Unit = store(offset, bytes.toArray)

  /**
   * Stores data at the given offset.
   * The memory is automatically expanded to accommodate new data - filling empty regions with zeroes if necessary -
   * hence an OOM error may be thrown.
   */
  def store(offset: Int, data: Array[Byte]) {
    if (data.length > 0 && offset >= 0 && offset.toLong + data.length.toLong <= Int.MaxValue) {
      expand(offset, data.length)
      System.arraycopy(data, 0, underlying, offset, data.length)
    }
  }

  def load(offset: Int): UInt256 = UInt256(doLoad(offset, UInt256.SIZE))
  def load(offset: Int, size: Int): ByteString = ByteString(doLoad(offset, size.toInt))

  /**
   * Returns a ByteString of a given size starting at the given offset of the Memory.
   * The memory is automatically expanded (with zeroes) when reading previously uninitialised regions,
   * hence an OOM error may be thrown.
   */
  private def doLoad(offset: Int, size: Int): Array[Byte] = {
    if (size > 0 && offset >= 0 && offset.toLong + size.toLong <= Int.MaxValue) {
      expand(offset, size)
      val data = Array.ofDim[Byte](size)
      System.arraycopy(underlying, offset, data, 0, size)
      data
    } else {
      Array()
    }
  }

  /**
   * This function will expand the Memory size as if storing data given the `offset` and `size`.
   * If the memory is already initialised at that region it will not be modified, otherwise it will be filled with
   * zeroes.
   * This is required to satisfy memory expansion semantics for *CALL* opcodes.
   */
  def expand(offset: Int, size: Int) {
    if (size > 0 && offset >= 0 && offset.toLong + size.toLong <= Int.MaxValue) {
      val end = offset + size
      if (end > underlying.length) {
        val extended = Array.ofDim[Byte](end) // will be auto filled with 0
        System.arraycopy(underlying, 0, extended, 0, underlying.length)
        underlying = extended
      }
    }
  }

  def size: Int = underlying.length

  override def equals(that: Any): Boolean = {
    that match {
      case that: Memory => Arrays.equals(this.underlying, that.underlying)
      case other        => false
    }
  }

  override def hashCode: Int = underlying.hashCode
  override def toString: String = s"Memory(${khipu.toHexString(underlying)})"
}

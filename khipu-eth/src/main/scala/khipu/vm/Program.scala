package khipu.vm

import akka.util.ByteString
import khipu.Hash
import khipu.crypto
import scala.annotation.tailrec

/**
 * Holds a program's code and provides utilities for accessing it (defaulting to zeroes when out of scope)
 *
 * @param code the EVM bytecode as bytes
 */
final case class Program(code: Array[Byte]) {
  val length = code.length

  lazy val codeHash = Hash(crypto.kec256(code))
  lazy val validJumpDestinations: Set[Int] = validJumpDestinationsAfterPosition(0)

  def getByte(pc: Int): Byte = if (pc >= 0 && pc < length) code(pc) else 0

  def getBytes(from: Int, size: Int): ByteString = {
    val slice = Array.ofDim[Byte](size) // auto filled with 0
    System.arraycopy(code, from, slice, 0, math.min(size, length - from))
    ByteString(slice)
  }

  /**
   * Returns the valid jump destinations of the program after a given position
   * See section 9.4.3 in Yellow Paper for more detail.
   *
   * @param pos from where to start searching for valid jump destinations in the code.
   * @param acc with the previously obtained valid jump destinations.
   */
  @tailrec
  private def validJumpDestinationsAfterPosition(pos: Int, acc: Set[Int] = Set()): Set[Int] = {
    if (pos >= 0 && pos < length) {
      val byte = code(pos)
      // we only need to check PushOp and JUMPDEST, they are both present in Frontier
      EvmConfig.FrontierConfig.byteToOpCode.get(byte) match {
        case Some(JUMPDEST)       => validJumpDestinationsAfterPosition(pos + 1, acc + pos)
        case Some(pushOp: PushOp) => validJumpDestinationsAfterPosition(pos + pushOp.i + 2, acc) // bypass PushOp's value positions 
        case _                    => validJumpDestinationsAfterPosition(pos + 1, acc)
      }
    } else {
      acc
    }
  }
}

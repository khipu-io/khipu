package khipu.vm

import akka.util.ByteString
import khipu.Hash
import khipu.crypto
import khipu.util.collection.IntSet

/**
 * Holds a program's code and provides utilities for accessing it (defaulting to zeroes when out of scope)
 *
 * @param code the EVM bytecode as bytes
 */
final case class Program(code: Array[Byte]) {
  val length = code.length

  lazy val codeHash = Hash(crypto.kec256(code))
  private lazy val validJumpDestinations: IntSet = generateValidJumpDestinations()

  def getByte(pc: Int): Byte = if (pc >= 0 && pc < length) code(pc) else 0
  def getBytes(from: Int, size: Int): ByteString = {
    val slice = Array.ofDim[Byte](size) // auto filled with 0
    System.arraycopy(code, from, slice, 0, math.min(size, length - from))
    ByteString(slice)
  }

  def isValidJumpDestination(pos: Int) = validJumpDestinations.contains(pos)

  /**
   * Returns the valid jump destinations of the program after a given position
   * See section 9.4.3 in Yellow Paper for more detail.
   *
   * @param pos from where to start searching for valid jump destinations in the code.
   * @param acc with the previously obtained valid jump destinations.
   */
  private def generateValidJumpDestinations() = {
    val res = new IntSet(256)
    var pos = 0
    while (pos < length) {
      val byte = code(pos)
      // we only need to check PushOp and JUMPDEST, and they are both present in Frontier
      EvmConfig.FrontierConfig.getOpCode(byte) match {
        case Some(JUMPDEST) =>
          res += pos
          pos += 1
        case Some(pushOp: PushOp) =>
          pos += pushOp.i + 2 // also bypass PushOp's value positions 
        case _ =>
          pos += 1
      }
    }
    res
  }
}

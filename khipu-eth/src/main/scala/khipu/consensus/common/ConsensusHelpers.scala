package khipu.consensus.common

import akka.util.ByteString

object ConsensusHelpers {

  def zeroLeftPad(input: ByteString, requiredLength: Int): ByteString = {
    val paddingByteCount = math.max(0, requiredLength - input.size)
    (ByteString(Array.ofDim[Byte](paddingByteCount) ++ input)).slice(0, requiredLength)
  }
}
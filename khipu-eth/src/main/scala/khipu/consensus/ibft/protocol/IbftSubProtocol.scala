package khipu.consensus.ibft.protocol

import khipu.consensus.ibft.messagedata.IbftV2
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.SubProtocol

object IbftSubProtocol {
  val NAME = "IBF"
  val IBFV1 = Capability.create(NAME, 1)

  val INVALID_MESSAGE_NAME = "invalid"

  private val INSTANCE = new IbftSubProtocol()
  def get(): IbftSubProtocol = INSTANCE
}
class IbftSubProtocol extends SubProtocol {
  import IbftSubProtocol._

  override def getName(): String = NAME

  override def messageSpace(protocolVersion: Int): Int = IbftV2.MESSAGE_SPACE

  override def isValidMessageCode(protocolVersion: Int, code: Int): Boolean = {
    code match {
      case IbftV2.PROPOSAL | IbftV2.PREPARE | IbftV2.COMMIT | IbftV2.ROUND_CHANGE => true
      case _ => false
    }
  }

  override def messageName(protocolVersion: Int, code: Int): String = {
    code match {
      case IbftV2.PROPOSAL     => "Proposal"
      case IbftV2.PREPARE      => "Prepare"
      case IbftV2.COMMIT       => "Commit"
      case IbftV2.ROUND_CHANGE => "RoundChange"
      case _                   => INVALID_MESSAGE_NAME
    }
  }
}

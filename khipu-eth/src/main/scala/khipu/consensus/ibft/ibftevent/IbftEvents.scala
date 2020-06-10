package khipu.consensus.ibft.ibftevent

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;

/** Static helper functions for producing and working with IbftEvent objects */
object IbftEvents {
  def fromMessage(message: Message): IbftEvent = {
    new IbftReceivedMessageEvent(message);
  }

  trait Type
  object Type {
    case object ROUND_EXPIRY extends Type
    case object NEW_CHAIN_HEAD extends Type
    case object BLOCK_TIMER_EXPIRY extends Type
    case object MESSAGE extends Type
  }
}

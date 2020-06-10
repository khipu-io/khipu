package khipu.consensus.ibft.ibftevent

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;

final case class IbftReceivedMessageEvent(message: Message) extends IbftEvent {

  override def getType(): IbftEvents.Type = IbftEvents.Type.MESSAGE;
}

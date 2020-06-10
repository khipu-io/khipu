package khipu.consensus.ibft.messagedata

import org.hyperledger.besu.consensus.ibft.messagewrappers.Prepare;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;


object PrepareMessageData {
  def fromMessageData(messageData: MessageData ): PrepareMessageData = {
    AbstractIbftMessageData.fromMessageData(
        messageData, MESSAGE_CODE, classOf[PrepareMessageData], PrepareMessageData::new);
  }

  def create(preapare: Prepare ): PrepareMessageData  {
    new PrepareMessageData(preapare.encode());
  }
}
class PrepareMessageData private (data: ByteString ) extends AbstractIbftMessageData(data) {

  def decode(): Prepare = Prepare.decode(data)

  override def getCode() = IbftV2.PREPARE 
}
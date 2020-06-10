/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package khipu.consensus.ibft.messagedata

import org.hyperledger.besu.consensus.ibft.messagewrappers.RoundChange;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;


object RoundChangeMessageData {

  def fromMessageData(messageData: MessageData ):RoundChangeMessageData =  {
    AbstractIbftMessageData.fromMessageData(
        messageData, MESSAGE_CODE, classOf[RoundChangeMessageData], RoundChangeMessageData::new);
  }


  def create(signedPayload: RoundChange ):RoundChangeMessageData =  {
    new RoundChangeMessageData(signedPayload.encode());
  }

  
}
class RoundChangeMessageData  private (data: ByteString ) extends AbstractIbftMessageData(data) {

  def decode(): RoundChange =  RoundChange.decode(data);

  override def getCode() = IbftV2.ROUND_CHANGE;
}

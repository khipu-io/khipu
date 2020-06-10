package khipu.consensus.ibft.messagedata

import org.hyperledger.besu.consensus.ibft.messagewrappers.Proposal;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;


object ProposalMessageData {

  def fromMessageData(messageData: MessageData ) : ProposalMessageData = {
    AbstractIbftMessageData.fromMessageData(
        messageData, MESSAGE_CODE, classOf[ProposalMessageData], ProposalMessageData::new);
  }

  def create(proposal:Proposal ): ProposalMessageData =  {
    new ProposalMessageData(proposal.encode());
  }

  
}
class ProposalMessageData private (data: ByteString ) extends AbstractIbftMessageData(data) {

  def decode(): Proposal = Proposal.decode(data);

  override def getCode() = IbftV2.PROPOSAL
}

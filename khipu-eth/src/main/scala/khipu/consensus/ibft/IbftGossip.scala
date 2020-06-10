package khipu.consensus.ibft

import khipu.consensus.ibft.messagedata.CommitMessageData
import khipu.consensus.ibft.messagedata.IbftV2
import khipu.consensus.ibft.messagedata.PrepareMessageData
import khipu.consensus.ibft.messagedata.ProposalMessageData
import khipu.consensus.ibft.messagedata.RoundChangeMessageData
import khipu.consensus.ibft.payload.Authored
import org.hyperledger.besu.consensus.ibft.network.ValidatorMulticaster;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * Class responsible for rebroadcasting IBFT messages to known validators
 * @param multicaster Network connections to the remote validators
 */
class IbftGossipf(multicaster: ValidatorMulticaster) extends Gossiper {

  /**
   * Retransmit a given IBFT message to other known validators nodes
   *
   * @param message The raw message to be gossiped
   */
  def send(message: Message) {
    val messageData = message.getData();
    val decodedMessage: Authored = messageData.getCode() match {
      case IbftV2.PROPOSAL =>
        ProposalMessageData.fromMessageData(messageData).decode();
      case IbftV2.PREPARE =>
        PrepareMessageData.fromMessageData(messageData).decode();
      case IbftV2.COMMIT =>
        CommitMessageData.fromMessageData(messageData).decode();
      case IbftV2.ROUND_CHANGE =>
        RoundChangeMessageData.fromMessageData(messageData).decode();
      case _ =>
        throw new IllegalArgumentException("Received message does not conform to any recognised IBFT message structure.")
    }
    val excludeAddressesList = Lists.newArrayList(
      message.getConnection().getPeerInfo().getAddress(), decodedMessage.getAuthor()
    );

    multicaster.send(messageData, excludeAddressesList);
  }
}

package khipu.consensus.ibft.messagedata

import org.hyperledger.besu.consensus.ibft.messagewrappers.Commit;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;


object CommitMessageData {
  def fromMessageData(messageData: MessageData ):CommitMessageData =  {
    return AbstractIbftMessageData.fromMessageData(
        messageData, MESSAGE_CODE, classOf[CommitMessageData], CommitMessageData::new);
  }

  def create(commit: Commit ): CommitMessageData =  {
    return new CommitMessageData(commit.encode());
  }

}
class CommitMessageData private (data: ByteString) extends AbstractIbftMessageData(data) {

  def decode(): Commit =  Commit.decode(data)

  override def getCode() = IbftV2.COMMIT
}

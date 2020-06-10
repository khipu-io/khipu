package khipu.consensus.ibft.payload

import org.hyperledger.besu.consensus.ibft.messagedata.IbftV2;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;
import java.util.StringJoiner;

object PreparePayload {
  def readFrom(rlpInput:RLPInput ):PreparePayload =  {
    rlpInput.enterList();
    final ConsensusRoundIdentifier roundIdentifier = ConsensusRoundIdentifier.readFrom(rlpInput);
    final Hash digest = Payload.readDigest(rlpInput);
    rlpInput.leaveList();
    PreparePayload(roundIdentifier, digest);
  }


}
final case class PreparePayload(roundIdentifier: ConsensusRoundIdentifier , digest:Hash ) extends  Payload {
  override  def writeTo(rlpOutput:RLPOutput ) {
    rlpOutput.startList();
    roundIdentifier.writeTo(rlpOutput);
    rlpOutput.writeBytes(digest);
    rlpOutput.endList();
  }

  override def getMessageType() = IbftV2.PREPARE;

}

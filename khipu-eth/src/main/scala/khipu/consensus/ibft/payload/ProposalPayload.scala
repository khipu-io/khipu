package khipu.consensus.ibft.payload

import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;
import java.util.StringJoiner;

object ProposalPayload {
  def readFrom(rlpInput:RLPInput ):ProposalPayload = {
    rlpInput.enterList();
    final ConsensusRoundIdentifier roundIdentifier = ConsensusRoundIdentifier.readFrom(rlpInput);
    final Hash digest = Hash.wrap(rlpInput.readBytes32());
    rlpInput.leaveList();

    return new ProposalPayload(roundIdentifier, digest);
  }


}
final case class ProposalPayload(roundIdentifier:ConsensusRoundIdentifier , digest:Hash ) extends  Payload {
  override def writeTo(rlpOutput:RLPOutput ) {
    rlpOutput.startList();
    roundIdentifier.writeTo(rlpOutput);
    rlpOutput.writeBytes(digest);
    rlpOutput.endList();
  }

  override def getMessageType() =IbftV2.PROPOSAL 
}

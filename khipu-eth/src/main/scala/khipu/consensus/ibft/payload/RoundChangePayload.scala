package khipu.consensus.ibft.payload

import org.hyperledger.besu.consensus.ibft.ConsensusRoundIdentifier;
import org.hyperledger.besu.consensus.ibft.messagedata.IbftV2;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

object RoundChangePayload {
  def readFrom(rlpInput:RLPInput ): RoundChangePayload =  {
    rlpInput.enterList();
    final ConsensusRoundIdentifier roundIdentifier = ConsensusRoundIdentifier.readFrom(rlpInput);

    final Optional<PreparedCertificate> preparedCertificate;

    if (rlpInput.nextIsNull()) {
      rlpInput.skipNext();
      preparedCertificate = Optional.empty();
    } else {
      preparedCertificate = Optional.of(PreparedCertificate.readFrom(rlpInput));
    }
    rlpInput.leaveList();

    RoundChangePayload(roundIdentifier, preparedCertificate);
  }


}
final case class RoundChangePayload(roundIdentifier: ConsensusRoundIdentifier , preparedCertificate: Option[PreparedCertificate] ) extends  Payload {
  private static final int TYPE = IbftV2.ROUND_CHANGE;

  override def writeTo(rlpOutput: RLPOutput ) {
    // RLP encode of the message data content (round identifier and prepared certificate)
    rlpOutput.startList();
    roundChangeIdentifier.writeTo(rlpOutput);

    if (preparedCertificate.isPresent()) {
      preparedCertificate.get().writeTo(rlpOutput);
    } else {
      rlpOutput.writeNull();
    }
    rlpOutput.endList();
  }

  override def getMessageType() = IbftV2.ROUND_CHANGE;
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package khipu.consensus.ibft.payload

import khipu.consensus.ibft.ConsensusRoundIdentifier;
import khipu.consensus.ibft.messagedata.IbftV2;
import org.hyperledger.besu.crypto.SECP256K1.Signature;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;
import java.util.StringJoiner;

object CommitPayload {
  private val TYPE = IbftV2.COMMIT;
  def readFrom(rlpInput: RLPInput): CommitPayload = {
    rlpInput.enterList();
    val roundIdentifier = ConsensusRoundIdentifier.readFrom(rlpInput);
    val digest = Payload.readDigest(rlpInput);
    val commitSeal = rlpInput.readBytes(Signature :: decode);
    rlpInput.leaveList();

    return new CommitPayload(roundIdentifier, digest, commitSeal);
  }

}
final case class CommitPayload(
    roundIdentifier: ConsensusRoundIdentifier,
    digest:          Hash,
    commitSeal:      Signature
) extends Payload {

  override def writeTo(rlpOutput: RLPOutput) {
    rlpOutput.startList();
    roundIdentifier.writeTo(rlpOutput);
    rlpOutput.writeBytes(digest);
    rlpOutput.writeBytes(commitSeal.encodedBytes());
    rlpOutput.endList();
  }

  override def getMessageType() = CommitPayload.TYPE;
}

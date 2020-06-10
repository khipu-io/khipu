package khipu.consensus.ibft

import khipu.domain.Address
import org.hyperledger.besu.consensus.common.VoteType;
import org.hyperledger.besu.ethereum.core.Address;
import org.hyperledger.besu.ethereum.rlp.RLPException;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

import java.util.Objects;

import com.google.common.collect.ImmutableBiMap;


/**
 * This class is only used to serialise/deserialise BlockHeaders and should not appear in business
 * logic.
 */
object Vote {
   val ADD_BYTE_VALUE = 0xFF.toByte
  val DROP_BYTE_VALUE = 0x0L.toByte

  private def voteToValue: ImmutableBiMap[VoteType, Byte] =
      ImmutableBiMap.of(
          VoteType.ADD, ADD_BYTE_VALUE,
          VoteType.DROP, DROP_BYTE_VALUE); 

    def authVote(address:Address ): Vote =  {
    new Vote(address, VoteType.ADD);
  }

  def dropVote(address:Address ): Vote =  {
    new Vote(address, VoteType.DROP);
  }

  def readFrom(rlpInput:RLPInput ): Vote =  {
    rlpInput.enterList();
    final Address recipient = Address.readFrom(rlpInput);
    final VoteType vote = voteToValue.inverse().get(rlpInput.readByte());
    if (vote == null) {
      throw new RLPException("Vote field was of an incorrect binary value.");
    }
    rlpInput.leaveList();

    return new Vote(recipient, vote);
  }

}
final case class Vote(recipient:Address , voteType:VoteType ) {

  def isAuth(): Boolean = {
    voteType.equals(VoteType.ADD);
  }

  def isDrop(): Boolean = {
    voteType.equals(VoteType.DROP);
  }

  def writeTo(rlpOutput: RLPOutput ) {
    rlpOutput.startList();
    rlpOutput.writeBytes(recipient);
    rlpOutput.writeByte(voteToValue.get(voteType));
    rlpOutput.endList();
  }

  
}

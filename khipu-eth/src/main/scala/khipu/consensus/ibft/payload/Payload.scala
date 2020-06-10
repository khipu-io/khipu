package khipu.consensus.ibft.payload

import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPOutput;

object Payload {
  def readDigest(ibftMessageData: RLPInput): Hash = {
    Hash.wrap(ibftMessageData.readBytes32());
  }
}

trait Payload extends RoundSpecific {

  def writeTo(rlpOutput: RLPOutput): Unit

  def encoded(): ByteString = {
    val rlpOutput = new BytesValueRLPOutput();
    writeTo(rlpOutput);

    rlpOutput.encoded();
  }

  def getMessageType(): Int
}

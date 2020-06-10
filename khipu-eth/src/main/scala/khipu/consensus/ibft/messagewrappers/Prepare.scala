package khipu.consensus.ibft.messagewrappers

import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;

import khipu.consensus.ibft.payload.PreparePayload
import khipu.consensus.ibft.payload.SignedData

object Prepare {
  def decode(data: ByteString): Prepare = {
    new Prepare(SignedData.readSignedPreparePayloadFrom(RLP.input(data)));
  }

}
class Prepare(payload: SignedData[PreparePayload]) extends IbftMessage[PreparePayload](payload) {

  def getDigest(): Hash = {
    getPayload().digest;
  }

}

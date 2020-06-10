package khipu.consensus.ibft.messagewrappers

import org.hyperledger.besu.crypto.SECP256K1.Signature;
import org.hyperledger.besu.ethereum.core.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;

import khipu.consensus.ibft.payload.CommitPayload
import khipu.consensus.ibft.payload.SignedData

object Commit {
  def decode(data: ByteString): Commit = {
    new Commit(SignedData.readSignedCommitPayloadFrom(RLP.input(data)));
  }

}
class Commit(payload: SignedData[CommitPayload]) extends IbftMessage[CommitPayload](payload) {

  def getCommitSeal(): Signature = {
    getPayload().commitSeal;
  }

  def getDigest(): Hash = {
    getPayload().digest
  }

}

package khipu.consensus.ibft

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Message;

trait Gossiper {
  def send(message: Message)
}

package khipu.consensus.ibft.network

import org.hyperledger.besu.ethereum.p2p.rlpx.wire.MessageData;

import java.util.Collection;

trait ValidatorMulticaster {

  def send(message: MessageData)

  def send(message: MessageData, blackList: Collection[Address]);
}
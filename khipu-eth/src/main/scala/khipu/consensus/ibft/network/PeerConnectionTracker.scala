package khipu.consensus.ibft.network

import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;

trait PeerConnectionTracker {

  def add(newConnection: PeerConnection);

  def remove(removedConnection: PeerConnection);
}

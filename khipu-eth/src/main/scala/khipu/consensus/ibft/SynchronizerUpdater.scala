package khipu.consensus.ibft

import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;

trait SynchronizerUpdater {

  def updatePeerChainState(knownBlockNumber: Long, peerConnection: PeerConnection);
}

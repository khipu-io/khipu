package khipu.consensus.ibft

import org.hyperledger.besu.ethereum.eth.manager.EthPeer;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.p2p.rlpx.connections.PeerConnection;

class EthSynchronizerUpdater(ethPeers: EthPeers) extends SynchronizerUpdater {

  override def updatePeerChainState(knownBlockNumber: Long, peerConnection: PeerConnection) {
    val ethPeer = ethPeers.peer(peerConnection);
    if (ethPeer == null) {
      //LOG.debug("Received message from a peer with no corresponding EthPeer.");
      return ;
    }
    ethPeer.chainState().updateHeightEstimate(knownBlockNumber);
  }
}

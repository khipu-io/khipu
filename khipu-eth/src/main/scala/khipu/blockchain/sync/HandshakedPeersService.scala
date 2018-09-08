package khipu.blockchain.sync

import khipu.network.handshake.EtcHandshake.PeerInfo
import khipu.network.p2p.messages.WireProtocol.Disconnect
import khipu.network.rlpx.IncomingPeer
import khipu.network.rlpx.OutgoingPeer
import khipu.network.rlpx.Peer
import khipu.network.rlpx.PeerEntity
import khipu.network.rlpx.PeerManager
import khipu.store.AppStateStorage
import khipu.util
import scala.concurrent.duration.FiniteDuration

object HandshakedPeersService {
  final case class BlacklistPeer(peerId: String, reason: String, always: Boolean = false)

  private case class UnblacklistPeerTask(peerId: String)
  private case class UnblacklistPeerTick(peerId: String)
}
trait HandshakedPeersService { _: SyncService =>
  import context.dispatcher
  import HandshakedPeersService._

  protected def appStateStorage: AppStateStorage

  protected var handshakedPeers = Map[String, (Peer, PeerInfo)]()
  protected def incomingPeers = handshakedPeers.filter(_._2._1.isInstanceOf[IncomingPeer])
  protected def outgoingPeers = handshakedPeers.filter(_._2._1.isInstanceOf[OutgoingPeer])
  protected var blacklistPeers = Set[String]()
  protected var blacklistCounts = Map[String, Int]()

  def peerUpdateBehavior: Receive = {
    case PeerEntity.PeerHandshaked(peer, peerInfo) =>
      if (peerInfo.forkAccepted) {
        log.debug(s"[sync] added handshaked peer: $peer")
        handshakedPeers += (peer.id -> (peer, peerInfo))
        log.debug(s"[sync] handshaked peers: ${handshakedPeers.size}")
      }

    case PeerEntity.PeerDisconnected(peerId) if handshakedPeers.contains(peerId) =>
      log.debug(s"[sync] peer $peerId disconnected")
      removePeer(peerId)

    case PeerEntity.PeerInfoUpdated(peerId, peerInfo) =>
      log.debug(s"[sync] UpdatedPeerInfo: $peerInfo")
      handshakedPeers.get(peerId) foreach {
        case (peer, _) =>
          if (peerInfo.maxBlockNumber > appStateStorage.getEstimatedHighestBlock) {
            appStateStorage.putEstimatedHighestBlock(peerInfo.maxBlockNumber)
          }

          if (!peerInfo.forkAccepted) {
            log.debug(s"[sync] peer $peerId is not running the accepted fork, disconnecting")
            val disconnect = Disconnect(Disconnect.Reasons.UselessPeer)
            peer.entity ! PeerEntity.MessageToPeer(peerId, disconnect)
            removePeer(peerId)
            blacklist(peerId, util.Config.Sync.blacklistDuration, disconnect.toString, always = true)
          } else {
            handshakedPeers += (peerId -> (peer, peerInfo))
          }
      }

    case BlacklistPeer(peerId, reason, always) =>
      blacklist(peerId, util.Config.Sync.blacklistDuration, reason, always)

    case UnblacklistPeerTick(peerId) =>
      timers.cancel(UnblacklistPeerTask(peerId))
      blacklistPeers -= peerId

    case PeerEntity.PeerEntityStopped(peerId) =>
      log.debug(s"[sync] peer $peerId stopped")
      removePeer(peerId)
  }

  def removePeer(peerId: String) {
    log.debug(s"[sync] removing peer $peerId")
    timers.cancel(UnblacklistPeerTask(peerId))
    handshakedPeers -= peerId
  }

  def peersToDownloadFrom: Map[Peer, PeerInfo] =
    handshakedPeers.collect {
      case (peerId, (peer, info)) if !isBlacklisted(peerId) => (peer, info)
    }

  private def blacklist(peerId: String, duration: FiniteDuration, reason: String, always: Boolean = false) {
    log.debug(s"[sync] blacklisting peer $peerId for $duration, $reason")
    timers.cancel(UnblacklistPeerTask(peerId))

    val blacklistCount = blacklistCounts.getOrElse(peerId, 0)
    if (always || blacklistCount >= 3) {
      val peerToDisconnect = handshakedPeers.get(peerId) flatMap {
        case (peer: OutgoingPeer, _) => Some(peer)
        case (peer: IncomingPeer, _) => if (always) Some(peer) else None
      }

      peerToDisconnect map { peer =>
        log.debug(s"[sync] drop peer $peerId, $reason")
        val disconnect = Disconnect(Disconnect.Reasons.UselessPeer)
        peer.entity ! PeerEntity.MessageToPeer(peerId, disconnect)
        peerManager ! PeerManager.DropNode(peerId)
        removePeer(peerId)
        blacklistPeers -= peerId
        blacklistCounts -= peerId
      }

    } else {
      timers.startSingleTimer(UnblacklistPeerTask(peerId), UnblacklistPeerTick(peerId), duration)
      blacklistPeers += peerId
      blacklistCounts += (peerId -> (blacklistCount + 1))
    }
  }

  def isBlacklisted(peerId: String): Boolean = blacklistPeers.contains(peerId) || blacklistCounts.getOrElse(peerId, 0) >= 4
}

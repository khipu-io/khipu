package khipu.blockchain.sync

import akka.pattern.AskTimeoutException
import khipu.config.KhipuConfig
import khipu.domain.BlockHeader
import khipu.network.handshake.EtcHandshake.PeerInfo
import khipu.network.p2p.messages.WireProtocol.Disconnect
import khipu.network.rlpx.IncomingPeer
import khipu.network.rlpx.OutgoingPeer
import khipu.network.rlpx.Peer
import khipu.network.rlpx.PeerEntity
import khipu.network.rlpx.PeerManager
import khipu.storage.AppStateStorage
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success

object HandshakedPeersService {
  final case class BlacklistPeer(peerId: String, reason: String, always: Boolean = false)

  private case class UnblacklistPeerTask(peerId: String)
  private case class UnblacklistPeerTick(peerId: String)
}
trait HandshakedPeersService { _: SyncService =>
  import context.dispatcher
  import HandshakedPeersService._
  import KhipuConfig.Sync._

  protected def appStateStorage: AppStateStorage

  protected var handshakedPeers = Map[String, (Peer, PeerInfo)]()
  protected def incomingPeers = handshakedPeers.filter(_._2._1.isInstanceOf[IncomingPeer])
  protected def outgoingPeers = handshakedPeers.filter(_._2._1.isInstanceOf[OutgoingPeer])
  protected var blacklistPeers = Set[String]()
  protected var blacklistCounts = Map[String, Int]()

  protected var blockHeaderForChecking: Option[BlockHeader] = None
  protected var headerWhitePeers = Set[Peer]()
  protected var headerBlackPeers = Set[Peer]()

  def peerUpdateBehavior: Receive = {
    case PeerEntity.PeerHandshaked(peer, peerInfo) =>
      if (peerInfo.forkAccepted) {
        log.debug(s"[sync] added handshaked peer: $peer")
        handshakedPeers += (peer.id -> (peer, peerInfo))
        log.debug(s"[sync] handshaked peers: ${handshakedPeers.size}")

        blockHeaderForChecking map checkPeerByBlockHeader(peer) map { f =>
          f map {
            case true  => headerWhitePeers += peer
            case false =>
          }
        }
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
            blacklist(peerId, KhipuConfig.Sync.blacklistDuration, disconnect.toString, always = true)
          } else {
            handshakedPeers += (peerId -> (peer, peerInfo))
            if (!headerWhitePeers.contains(peer)) {
              blockHeaderForChecking map checkPeerByBlockHeader(peer) map { f =>
                f map {
                  case true  => headerWhitePeers += peer
                  case false =>
                }
              }
            }
          }
      }

    case BlacklistPeer(peerId, reason, always) =>
      blacklist(peerId, KhipuConfig.Sync.blacklistDuration, reason, always)

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
    headerWhitePeers = headerWhitePeers.filterNot(_.id == peerId)
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

  private def checkPeerByBlockHeader(peer: Peer)(targetBlockHeader: BlockHeader): Future[Boolean] = {
    requestingHeaders(peer, None, Left(targetBlockHeader.number), 1, 0, reverse = false)(20.seconds) transform {
      case Success(Some(BlockHeadersResponse(peerId, headers, true))) =>
        headers.find(_.number == targetBlockHeader.number) match {
          case Some(blockHeader) =>
            Success(blockHeader == targetBlockHeader)
          case None =>
            Success(false)
        }

      case Success(Some(BlockHeadersResponse(peerId, _, false))) =>
        Success(false)

      case Success(None) =>
        Success(false)

      case Failure(e: AskTimeoutException) =>
        Success(false)

      case Failure(e) =>
        Success(false)
    }
  }

  protected def setCurrBlockHeaderForChecking() {
    val bestBlockNumber = appStateStorage.getBestBlockNumber
    blockHeaderForChecking = blockchain.getBlockHeaderByNumber(bestBlockNumber - blockResolveDepth)
  }
}

package khipu.blockchain.sync

import akka.pattern.AskTimeoutException
import khipu.blockchain.sync.SyncService.SuspendPeerTick
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
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

object HandshakedPeersService {
  final case class BlacklistPeer(peerId: String, reason: String, force: Boolean = false)
  final case class ResetBlacklistCount(peerId: String)
}
trait HandshakedPeersService { _: SyncService =>
  import context.dispatcher
  import HandshakedPeersService._
  import KhipuConfig.Sync._

  protected def appStateStorage: AppStateStorage

  protected var handshakedPeers = Map[String, (Peer, PeerInfo)]()
  protected def incomingPeers = handshakedPeers.filter(_._2._1.isInstanceOf[IncomingPeer])
  protected def outgoingPeers = handshakedPeers.filter(_._2._1.isInstanceOf[OutgoingPeer])
  protected val suspendedPeers = new mutable.HashMap[String, Long]()
  protected val blacklistCounts = new mutable.HashMap[String, Int]()

  protected var blockHeaderForChecking: Option[BlockHeader] = None
  protected var headerWhitePeers = Set[Peer]()
  protected var headerBlackPeers = Set[Peer]()

  private val blacklistDuration = KhipuConfig.Sync.blacklistDuration.toMillis

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
      } else {
        blacklist(peer, "blacklisted or not running the accepted fork", force = true)
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
            blacklist(peer, "not running the accepted fork", force = true)
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

    case ResetBlacklistCount(peerId) =>
      blacklistCounts(peerId) = 0

    case BlacklistPeer(peerId, reason, always) =>
      handshakedPeers.get(peerId) map { case (peer, _) => blacklist(peer, reason, always) }

    case SuspendPeerTick =>
      val now = System.currentTimeMillis
      val toRelease = suspendedPeers.collect {
        case (peerId, startTime) if (now - startTime) > blacklistDuration => peerId
      }

      if (toRelease.nonEmpty) {
        suspendedPeers --= toRelease
        log.debug(s"Released $toRelease, suspended ${suspendedPeers.map(_._1)}")
      }

    case PeerEntity.PeerEntityStopped(peerId) =>
      log.debug(s"[sync] peer $peerId stopped")
      removePeer(peerId)
  }

  def goodPeers: Map[Peer, PeerInfo] = {
    handshakedPeers.collect {
      case (peerId, (peer, info)) if !isBlacklisted(peerId) && !isSuspended(peerId) => (peer, info)
    }
  }

  private def toDropOneBlacklisted() = {
    val nHandshakedPeers = handshakedPeers.size
    if (nHandshakedPeers > peerConfiguration.maxPeers * 0.8) {
      val nGoodPeers = handshakedPeers.filterNot(x => isBlacklisted(x._1)).size
      if (nGoodPeers < nHandshakedPeers * 0.5) {
        blacklistCounts.toList.filter(x => x._2 >= 3 && handshakedPeers.contains(x._1)).sortBy(-_._2).headOption foreach {
          case (peerId, count) =>
            handshakedPeers.get(peerId) foreach { x => dropPeer(x._1, s"blacklist count $count") }
        }
      }
    }
  }

  private def dropPeer(peer: Peer, reason: String) {
    log.debug(s"[sync] drop peer ${peer.id}, $reason")
    val disconnect = Disconnect(Disconnect.Reasons.UselessPeer)
    peer.entity ! PeerEntity.MessageToPeer(peer.id, disconnect)
    peerManager ! PeerManager.DropNode(peer.id)
    removePeer(peer.id)
  }

  private def removePeer(peerId: String) {
    log.debug(s"[sync] removing peer $peerId")
    suspendedPeers -= peerId
    handshakedPeers -= peerId
    headerWhitePeers = headerWhitePeers.filterNot(_.id == peerId)
  }

  private def isSuspended(peerId: String) = suspendedPeers.contains(peerId)
  private def isBlacklisted(peerId: String) = blacklistCounts.getOrElse(peerId, 0) >= 3

  private def blacklist(peer: Peer, reason: String, force: Boolean = false) {
    val blacklistCount = blacklistCounts.getOrElse(peer.id, 0)
    log.debug(s"[sync] blacklisting peer ${peer.id} (blacklisted $blacklistCount) ${if (force) "force" else blacklistDuration.millisecond}, $reason")

    blacklistCounts(peer.id) = blacklistCount + 1
    if (force) {
      dropPeer(peer, reason)
    } else {
      suspendedPeers(peer.id) = System.currentTimeMillis
      toDropOneBlacklisted()
    }

    log.debug(s"[sync] suspended: ${suspendedPeers.map(_._1)}, blacklisted: ${blacklistCounts.filter(_._2 >= 3).map(_._1)}, handshaked: ${handshakedPeers.map(_._2._1)}")
  }

  protected def setCurrBlockHeaderForChecking() {
    val bestBlockNumber = appStateStorage.getBestBlockNumber
    blockHeaderForChecking = blockchain.getBlockHeaderByNumber(bestBlockNumber - blockResolveDepth)
  }

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
}
